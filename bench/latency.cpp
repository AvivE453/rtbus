// One-way latency between two processes on this machine: rtbus, with a sleeping or a
// busy-polling subscriber, against a Unix domain stream socket, for four message sizes.
//
// The sender fills a message, stamps it with CLOCK_MONOTONIC and hands it to the transport.
// The receiver stamps it again once the whole message is available to it: when its callback
// starts for rtbus, when the last byte has been read for the socket. CLOCK_MONOTONIC is one
// clock for every process on the machine, so the difference is the one-way latency.
//
// Usage: rtbus_latency [samples] [csv-path]
//   samples   messages measured per case, one every millisecond (default 10000)
//   csv-path  where to write every sample, for plot_latency.py (default latency.csv)

#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <exception>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "rtbus/file_descriptor.hpp"
#include "rtbus/node.hpp"

namespace {

using Nanoseconds = std::int64_t;
using Latencies = std::vector<Nanoseconds>;

constexpr Nanoseconds kPeriod = 1'000'000;  // 1 kHz
// Messages sent before the measured ones, so first-touch page faults and cold caches stay
// out of the numbers.
constexpr std::size_t kWarmup = 200;
// The latency recorded for a message the receiver never saw.
constexpr Nanoseconds kLost = -1;

Nanoseconds now() {
  timespec time{};
  ::clock_gettime(CLOCK_MONOTONIC, &time);
  return Nanoseconds{time.tv_sec} * 1'000'000'000 + time.tv_nsec;
}

[[noreturn]] void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

struct Stamp {
  Nanoseconds sent;
  std::uint64_t sequence;
};

template <std::size_t Size>
struct Message {
  Stamp stamp;
  std::array<std::byte, Size - sizeof(Stamp)> payload;
};

// Keeps to an absolute schedule, so one late message does not push back all the others.
class Pacer {
 public:
  Pacer() : next_(now()) {}

  void wait_for_next_tick() {
    next_ += kPeriod;
    timespec deadline{};
    deadline.tv_sec = next_ / 1'000'000'000;
    deadline.tv_nsec = next_ % 1'000'000'000;
    while (::clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &deadline, nullptr) == EINTR) {
    }
  }

 private:
  Nanoseconds next_;
};

// The sending side of one measurement, in a child process. Fork only while single-threaded:
// the child gets only the forking thread, so a lock another thread held would stay locked.
class SenderProcess {
 public:
  explicit SenderProcess(const std::function<void()>& send) : pid_(::fork()) {
    if (pid_ == -1) {
      throw_errno("fork");
    }
    if (pid_ == 0) {
      // Leave only via _exit: returning would run the rest of the parent's program here.
      try {
        send();
      } catch (const std::exception& error) {
        std::cerr << "sender: " << error.what() << '\n';
        ::_exit(1);
      }
      ::_exit(0);
    }
  }

  ~SenderProcess() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      ::waitpid(pid_, nullptr, 0);
    }
  }

  SenderProcess(const SenderProcess&) = delete;
  SenderProcess& operator=(const SenderProcess&) = delete;
  SenderProcess(SenderProcess&&) = delete;
  SenderProcess& operator=(SenderProcess&&) = delete;

  // Waits for the sender to finish. Throws if it failed.
  void join() {
    int status = 0;
    if (::waitpid(pid_, &status, 0) != pid_) {
      throw_errno("waitpid");
    }
    pid_ = -1;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      throw std::runtime_error("the sender failed");
    }
  }

 private:
  pid_t pid_;
};

// ---- rtbus ----

template <std::size_t Size>
void send_over_rtbus(const std::string& topic, std::size_t count) {
  rtbus::Node node("latency_sender");
  auto publisher = node.advertise<Message<Size>>(topic);
  while (publisher.subscriber_count() == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  Pacer pacer;
  for (std::uint64_t sequence = 0; sequence < count; ++sequence) {
    pacer.wait_for_next_tick();
    auto message = publisher.loan();
    message->payload.fill(static_cast<std::byte>(sequence));
    message->stamp = {now(), sequence};
    publisher.publish(std::move(message));
  }
}

template <std::size_t Size>
Latencies receive_over_rtbus(const std::string& topic, std::size_t count,
                             rtbus::WaitMode wait_mode) {
  Latencies latencies(count, kLost);
  std::promise<void> last_arrived;
  std::future<void> done = last_arrived.get_future();
  rtbus::Node node("latency_receiver");
  rtbus::SubscriberOptions options;
  options.wait_mode = wait_mode;
  const auto subscriber = node.subscribe<Message<Size>>(
      topic,
      [&](const Message<Size>& message) {
        const Nanoseconds arrived = now();
        latencies[message.stamp.sequence] = arrived - message.stamp.sent;
        if (message.stamp.sequence + 1 == count) {
          last_arrived.set_value();
        }
      },
      options);
  done.wait();
  return latencies;
}

template <std::size_t Size>
Latencies measure_rtbus(rtbus::WaitMode wait_mode, std::size_t count) {
  const std::string topic = "latency_" + std::to_string(::getpid()) + "_" + std::to_string(Size);
  SenderProcess sender([&] { send_over_rtbus<Size>(topic, count); });
  Latencies latencies = receive_over_rtbus<Size>(topic, count, wait_mode);
  sender.join();
  return latencies;
}

// ---- Unix domain socket ----

// A stream socket carries bytes, not messages, so each message goes out behind its length,
// as it would in a real protocol over a stream socket.
using Length = std::uint32_t;

void write_all(int fd, const std::byte* data, std::size_t size) {
  while (size > 0) {
    const ssize_t written = ::write(fd, data, size);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw_errno("write");
    }
    data += written;
    size -= static_cast<std::size_t>(written);
  }
}

// False if the stream ends before the first byte; an end in the middle is an error.
bool read_all(int fd, std::byte* data, std::size_t size) {
  std::size_t done = 0;
  while (done < size) {
    const ssize_t got = ::read(fd, data + done, size - done);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw_errno("read");
    }
    if (got == 0) {
      if (done == 0) {
        return false;
      }
      throw std::runtime_error("the stream ended in the middle of a message");
    }
    done += static_cast<std::size_t>(got);
  }
  return true;
}

template <std::size_t Size>
void send_over_socket(int fd, std::size_t count) {
  std::vector<std::byte> frame(sizeof(Length) + Size);
  const Length length = Size;
  std::memcpy(frame.data(), &length, sizeof(length));
  std::byte* const message = frame.data() + sizeof(Length);
  Pacer pacer;
  for (std::uint64_t sequence = 0; sequence < count; ++sequence) {
    pacer.wait_for_next_tick();
    std::fill(message + sizeof(Stamp), message + Size, static_cast<std::byte>(sequence));
    const Stamp stamp{now(), sequence};
    std::memcpy(message, &stamp, sizeof(stamp));
    write_all(fd, frame.data(), frame.size());
  }
}

Latencies receive_over_socket(int fd, std::size_t count) {
  Latencies latencies(count, kLost);
  std::vector<std::byte> message;
  Length length = 0;
  while (read_all(fd, reinterpret_cast<std::byte*>(&length), sizeof(length))) {
    message.resize(length);
    if (!read_all(fd, message.data(), length)) {
      throw std::runtime_error("the stream ended after a message length");
    }
    const Nanoseconds arrived = now();
    Stamp stamp{};
    std::memcpy(&stamp, message.data(), sizeof(stamp));
    latencies.at(stamp.sequence) = arrived - stamp.sent;
  }
  return latencies;
}

template <std::size_t Size>
Latencies measure_unix_socket(std::size_t count) {
  int ends[2];
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, ends) != 0) {
    throw_errno("socketpair");
  }
  rtbus::FileDescriptor receiving_end(ends[0]);
  rtbus::FileDescriptor sending_end(ends[1]);
  SenderProcess sender([&] {
    receiving_end.reset();
    send_over_socket<Size>(sending_end.get(), count);
  });
  // Closed here too, so the stream ends once the sender exits.
  sending_end.reset();
  Latencies latencies = receive_over_socket(receiving_end.get(), count);
  sender.join();
  return latencies;
}

// ---- Report ----

struct Case {
  std::string transport;
  std::size_t message_size;
  Latencies latencies;  // the measured messages only, lost ones removed, sorted
  std::size_t lost;
};

Case make_case(std::string transport, std::size_t message_size, const Latencies& all) {
  Latencies measured(all.begin() + kWarmup, all.end());
  const auto lost = static_cast<std::size_t>(std::count(measured.begin(), measured.end(), kLost));
  measured.erase(std::remove(measured.begin(), measured.end(), kLost), measured.end());
  std::sort(measured.begin(), measured.end());
  return {std::move(transport), message_size, std::move(measured), lost};
}

// The nearest-rank percentile: the smallest sample with at least `fraction` of the samples
// at or below it.
Nanoseconds percentile(const Latencies& sorted, double fraction) {
  const auto rank =
      static_cast<std::size_t>(std::ceil(fraction * static_cast<double>(sorted.size())));
  return sorted[std::max<std::size_t>(rank, 1) - 1];
}

constexpr std::size_t kKilobyte = 1024;
constexpr std::size_t kMegabyte = 1024 * kKilobyte;

std::string size_label(std::size_t bytes) {
  if (bytes >= kMegabyte) {
    return std::to_string(bytes / kMegabyte) + " MB";
  }
  if (bytes >= kKilobyte) {
    return std::to_string(bytes / kKilobyte) + " KB";
  }
  return std::to_string(bytes) + " B";
}

void print_table(const std::vector<Case>& cases) {
  std::printf("%-16s %6s %9s %9s %9s %9s %5s\n", "transport", "size", "p50 us", "p99 us",
              "p99.9 us", "max us", "lost");
  for (const Case& c : cases) {
    const auto micros = [](Nanoseconds ns) { return static_cast<double>(ns) / 1000.0; };
    std::printf("%-16s %6s %9.1f %9.1f %9.1f %9.1f %5zu\n", c.transport.c_str(),
                size_label(c.message_size).c_str(), micros(percentile(c.latencies, 0.50)),
                micros(percentile(c.latencies, 0.99)), micros(percentile(c.latencies, 0.999)),
                micros(c.latencies.back()), c.lost);
  }
}

void write_csv(const std::vector<Case>& cases, const std::string& path) {
  std::ofstream csv(path);
  csv << "transport,message_bytes,latency_ns\n";
  for (const Case& c : cases) {
    for (const Nanoseconds latency : c.latencies) {
      csv << c.transport << ',' << c.message_size << ',' << latency << '\n';
    }
  }
  if (!csv) {
    throw std::runtime_error("could not write " + path);
  }
}

template <std::size_t Size>
void measure_every_transport(std::size_t count, std::vector<Case>& cases) {
  const auto measure = [&](const char* transport, const std::function<Latencies()>& run) {
    std::cerr << "measuring " << transport << ", " << size_label(Size) << "...\n";
    cases.push_back(make_case(transport, Size, run()));
  };
  measure("unix-socket", [&] { return measure_unix_socket<Size>(count); });
  measure("rtbus-sleep", [&] { return measure_rtbus<Size>(rtbus::WaitMode::kSleep, count); });
  measure("rtbus-busy-poll",
          [&] { return measure_rtbus<Size>(rtbus::WaitMode::kBusyPoll, count); });
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const std::size_t samples = argc > 1 ? std::stoul(argv[1]) : 10'000;
    const std::string csv_path = argc > 2 ? argv[2] : "latency.csv";
    const std::size_t count = kWarmup + samples;

    std::vector<Case> cases;
    measure_every_transport<64>(count, cases);
    measure_every_transport<4 * kKilobyte>(count, cases);
    measure_every_transport<64 * kKilobyte>(count, cases);
    measure_every_transport<kMegabyte>(count, cases);

    print_table(cases);
    write_csv(cases, csv_path);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "rtbus_latency: " << error.what() << '\n';
    return 1;
  }
}
