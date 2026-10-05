// A randomized soak test: publishers that restart, subscribers that come and go, and
// subscriber processes killed with SIGKILL at random, for a given number of seconds.
//
// Every subscriber checks every message it receives. The payload must be exactly what its
// publisher wrote, since a chunk reused while still being read shows up as a torn payload.
// Messages from one publisher must arrive in order. A leaked chunk eventually makes loan()
// throw, and a segment left in /dev/shm at the end is a failure too. Exits 0 only if every
// check passed.
//
// Usage: rtbus_stress [seconds]            the coordinator (10 seconds by default)
//        rtbus_stress --subscriber TOPIC   a subscriber process, started by the coordinator
//
// ctest runs it for a few seconds. Before merging transport changes, run it for ten minutes
// under ThreadSanitizer:  ./build/tsan/tests/rtbus_stress 600
//
// What ThreadSanitizer can and cannot see here: it checks each process's own threads (the
// subscriber threads, their mutex, the callbacks). It cannot check the shared-memory protocol,
// because the publisher and each subscriber map the segment separately, at different
// addresses, and ThreadSanitizer tells memory apart by address. A weakened memory ordering in
// that protocol passes here (tried: a relaxed refcount release); the unit tests, which use a
// single mapping, are what catch it. This test catches what the protocol does wrong, not why.

#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "rtbus/node.hpp"

extern char** environ;

namespace rtbus {
namespace {

using Clock = std::chrono::steady_clock;

constexpr int kTopics = 3;
// Per topic. Together they stay below the 8 slots of a topic, so joining never fails: a
// slot left by a killed process is taken back rather than counted as full.
constexpr std::size_t kMaxInProcessSubscribers = 4;
constexpr std::size_t kSubscriberProcesses = 2;
constexpr int kCorruptionExitCode = 2;

struct Message {
  std::uint32_t incarnation;  // which publisher of the topic this is, counting from 1
  std::uint64_t sequence;     // per incarnation, counting from 1
  std::array<std::uint64_t, 62> words;
};

// splitmix64: every word of every message differs, so a payload mixed from two messages
// cannot pass for either.
std::uint64_t expected_word(const Message& message, std::size_t index) {
  std::uint64_t x = (std::uint64_t{message.incarnation} << 48) ^ (message.sequence << 8) ^ index;
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

void fill(Message& message) {
  for (std::size_t i = 0; i < message.words.size(); ++i) {
    message.words[i] = expected_word(message, i);
  }
}

// Checks the messages one subscriber receives.
class MessageChecker {
 public:
  // Empty if `message` is intact and in order, otherwise what is wrong with it.
  std::string check(const Message& message) {
    for (std::size_t i = 0; i < message.words.size(); ++i) {
      if (message.words[i] != expected_word(message, i)) {
        return "torn payload in " + describe(message.incarnation, message.sequence);
      }
    }
    if (message.incarnation < incarnation_ ||
        (message.incarnation == incarnation_ && message.sequence <= sequence_)) {
      return describe(message.incarnation, message.sequence) + " arrived after " +
             describe(incarnation_, sequence_);
    }
    incarnation_ = message.incarnation;
    sequence_ = message.sequence;
    return {};
  }

 private:
  static std::string describe(std::uint32_t incarnation, std::uint64_t sequence) {
    return "message " + std::to_string(incarnation) + "." + std::to_string(sequence);
  }

  std::uint32_t incarnation_ = 0;
  std::uint64_t sequence_ = 0;
};

// Now and then a slow callback, so that queues overflow while a message is held.
void simulate_work(const Message& message) {
  if (message.sequence % 997 == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

class Failures {
 public:
  void report(const std::string& what) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (++count_ <= kPrinted) {
      std::cerr << "FAILURE: " << what << '\n';
    }
  }

  [[nodiscard]] int count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return count_;
  }

 private:
  static constexpr int kPrinted = 20;
  mutable std::mutex mutex_;
  int count_ = 0;
};

struct Stats {
  std::atomic<std::uint64_t> published{0};
  std::atomic<std::uint64_t> discarded{0};
  std::atomic<std::uint64_t> received{0};
  std::atomic<std::uint64_t> publisher_restarts{0};
  std::atomic<std::uint64_t> joins{0};
  std::atomic<std::uint64_t> processes_killed{0};
};

struct Run {
  Clock::time_point deadline;
  Stats stats;
  Failures failures;
};

std::uint32_t roll(std::mt19937& random, std::uint32_t below) { return random() % below; }

// ---- The coordinator's threads ----

// Publishes as fast as it can, with an occasional pause so that subscribers fall asleep and
// must be woken, and replaces its publisher every few hundred milliseconds.
void run_publisher(const std::string& topic, Run& run, std::uint32_t seed) {
  std::mt19937 random(seed);
  Node node("stress_publisher");
  try {
    for (std::uint32_t incarnation = 1; Clock::now() < run.deadline; ++incarnation) {
      auto publisher = node.advertise<Message>(topic);
      ++run.stats.publisher_restarts;
      const auto retire_at = Clock::now() + std::chrono::milliseconds(100 + roll(random, 700));
      for (std::uint64_t sequence = 1; Clock::now() < retire_at; ++sequence) {
        auto message = publisher.loan();
        message->incarnation = incarnation;
        message->sequence = sequence;
        fill(*message);
        if (roll(random, 50) == 0) {
          ++run.stats.discarded;  // the loan goes back unpublished
          continue;
        }
        publisher.publish(std::move(message));
        ++run.stats.published;
        if (roll(random, 10) == 0) {
          std::this_thread::sleep_for(std::chrono::microseconds(roll(random, 500)));
        }
      }
    }
  } catch (const std::exception& error) {
    run.failures.report(topic + " publisher: " + error.what());
  }
}

Subscriber<Message> join(Node& node, const std::string& topic, Run& run, std::mt19937& random) {
  SubscriberOptions options;
  options.queue_capacity = 1 + roll(random, 16);
  options.wait_mode = roll(random, 8) == 0 ? WaitMode::kBusyPoll : WaitMode::kSleep;
  return node.subscribe<Message>(
      topic,
      [topic, &run, checker = MessageChecker()](const Message& message) mutable {
        ++run.stats.received;
        const std::string problem = checker.check(message);
        if (!problem.empty()) {
          run.failures.report(topic + ": " + problem);
        }
        simulate_work(message);
      },
      options);
}

// Keeps adding and removing subscribers of one topic in this process.
void churn_subscribers(const std::string& topic, Run& run, std::uint32_t seed) {
  std::mt19937 random(seed);
  Node node("stress_subscribers");
  std::vector<Subscriber<Message>> subscribers;
  try {
    while (Clock::now() < run.deadline) {
      const bool add = subscribers.empty() ||
                       (subscribers.size() < kMaxInProcessSubscribers && roll(random, 2) == 0);
      if (add) {
        subscribers.push_back(join(node, topic, run, random));
        ++run.stats.joins;
      } else {
        const auto count = static_cast<std::uint32_t>(subscribers.size());
        subscribers.erase(subscribers.begin() + roll(random, count));
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1 + roll(random, 50)));
    }
  } catch (const std::exception& error) {
    run.failures.report(topic + " subscribers: " + error.what());
  }
}

pid_t spawn_subscriber_process(const std::string& topic) {
  std::string program = "rtbus_stress";
  std::string flag = "--subscriber";
  std::string topic_argument = topic;
  std::array<char*, 4> argv = {program.data(), flag.data(), topic_argument.data(), nullptr};
  pid_t pid = 0;
  // posix_spawn, not fork: this process has many threads, and a forked child would inherit
  // their locks in whatever state they were.
  const int error = ::posix_spawn(&pid, "/proc/self/exe", nullptr, nullptr, argv.data(), environ);
  if (error != 0) {
    throw std::system_error(error, std::generic_category(), "posix_spawn");
  }
  return pid;
}

struct SubscriberProcess {
  pid_t pid;
  std::string topic;
};

// A subscriber process must die only by our SIGKILL. Anything else means it found a bad
// message (kCorruptionExitCode) or crashed.
void check_death(const SubscriberProcess& process, int status, Run& run) {
  if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) {
    return;
  }
  const std::string how = WIFEXITED(status) ? "exited with " + std::to_string(WEXITSTATUS(status))
                                            : "died of signal " + std::to_string(WTERMSIG(status));
  run.failures.report("subscriber process of " + process.topic + " " + how);
}

void kill_and_reap(const SubscriberProcess& process, Run& run) {
  ::kill(process.pid, SIGKILL);
  int status = 0;
  ::waitpid(process.pid, &status, 0);
  check_death(process, status, run);
}

// Keeps kSubscriberProcesses running for every topic, killing a random one every so often.
void churn_subscriber_processes(const std::vector<std::string>& topics, Run& run,
                                std::uint32_t seed) {
  std::mt19937 random(seed);
  std::vector<SubscriberProcess> processes;
  try {
    for (const std::string& topic : topics) {
      for (std::size_t i = 0; i < kSubscriberProcesses; ++i) {
        processes.push_back({spawn_subscriber_process(topic), topic});
      }
    }
    while (Clock::now() < run.deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20 + roll(random, 180)));
      const auto count = static_cast<std::uint32_t>(processes.size());
      SubscriberProcess& victim = processes[roll(random, count)];
      kill_and_reap(victim, run);
      ++run.stats.processes_killed;
      victim.pid = spawn_subscriber_process(victim.topic);
    }
  } catch (const std::exception& error) {
    run.failures.report(std::string("subscriber processes: ") + error.what());
  }
  for (const SubscriberProcess& process : processes) {
    kill_and_reap(process, run);
  }
}

void check_segments_removed(const std::vector<std::string>& topics, Run& run) {
  for (const std::string& topic : topics) {
    if (::access(("/dev/shm/rtbus." + topic).c_str(), F_OK) == 0) {
      run.failures.report("the segment of " + topic + " was left in /dev/shm");
    }
  }
}

void print_summary(const Stats& stats, int failures) {
  std::cout << "published " << stats.published << " messages (" << stats.discarded
            << " loans discarded), in-process subscribers received " << stats.received << '\n'
            << "publishers started " << stats.publisher_restarts << ", subscribers joined "
            << stats.joins << ", subscriber processes killed " << stats.processes_killed << '\n'
            << "failures: " << failures << '\n';
}

int run_coordinator(std::chrono::seconds duration) {
  Run run;
  run.deadline = Clock::now() + duration;
  std::vector<std::string> topics;
  topics.reserve(kTopics);
  for (int i = 0; i < kTopics; ++i) {
    topics.push_back("stress_" + std::to_string(::getpid()) + "_" + std::to_string(i));
  }
  std::random_device seeds;
  std::vector<std::thread> threads;
  for (const std::string& topic : topics) {
    threads.emplace_back([&run, &topic, seed = seeds()] { run_publisher(topic, run, seed); });
    threads.emplace_back([&run, &topic, seed = seeds()] { churn_subscribers(topic, run, seed); });
  }
  threads.emplace_back(
      [&run, &topics, seed = seeds()] { churn_subscriber_processes(topics, run, seed); });
  for (std::thread& thread : threads) {
    thread.join();
  }
  check_segments_removed(topics, run);
  print_summary(run.stats, run.failures.count());
  return run.failures.count() == 0 ? 0 : 1;
}

// ---- A subscriber process ----

[[noreturn]] void run_subscriber_process(const std::string& topic) {
  Node node("stress_process");
  // Never read: it only has to stay alive until the coordinator kills this process.
  [[maybe_unused]] const auto subscriber = node.subscribe<Message>(
      topic, [topic, checker = MessageChecker()](const Message& message) mutable {
        const std::string problem = checker.check(message);
        if (!problem.empty()) {
          std::cerr << "FAILURE: " << topic << " (subscriber process): " << problem << '\n';
          ::_exit(kCorruptionExitCode);
        }
        simulate_work(message);
      });
  for (;;) {
    ::pause();  // until the coordinator kills us
  }
}

}  // namespace
}  // namespace rtbus

int main(int argc, char** argv) {
  try {
    const std::vector<std::string> arguments(argv + 1, argv + argc);
    if (arguments.size() == 2 && arguments[0] == "--subscriber") {
      rtbus::run_subscriber_process(arguments[1]);
    }
    const long seconds = arguments.empty() ? 10 : std::stol(arguments[0]);
    return rtbus::run_coordinator(std::chrono::seconds(seconds));
  } catch (const std::exception& error) {
    std::cerr << "rtbus_stress: " << error.what() << '\n';
    return 1;
  }
}
