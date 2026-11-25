// The cost of rtbus's publish path in one thread, with no process boundary and no wake-ups:
// what the CPU spends on it, apart from the scheduler.

#include <benchmark/benchmark.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rtbus/detail/index_ring.hpp"
#include "rtbus/detail/message_type.hpp"
#include "rtbus/detail/segment.hpp"
#include "rtbus/detail/segment_connection.hpp"
#include "rtbus/detail/shm_publisher.hpp"
#include "rtbus/detail/wake_signal.hpp"

namespace rtbus::detail {
namespace {

std::string unique_topic() {
  static int counter = 0;
  return "micro_" + std::to_string(::getpid()) + "_" + std::to_string(counter++);
}

// A made-up message type of any size: the transport only needs the size and a fingerprint
// both ends agree on.
MessageType message_of_size(std::size_t size) { return {0x6d6963726f, size}; }

// False if the pool ran dry, which would mean the benchmark leaks chunks.
bool publish_one(ShmPublisher& publisher) {
  const std::optional<std::uint32_t> chunk = publisher.loan();
  if (!chunk) {
    return false;
  }
  publisher.publish(*chunk);
  return true;
}

void take_and_finish(SegmentConnection& connection) {
  const std::optional<std::uint32_t> chunk = connection.take();
  if (chunk) {
    benchmark::DoNotOptimize(connection.payload(*chunk));
    connection.finish(*chunk);
  }
}

// Loan, publish to one subscriber, take and finish. The payload is never touched, so the
// time should not depend on the message size.
void publish_and_take(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  const std::string topic = unique_topic();
  ShmPublisher publisher(topic, message_of_size(size));
  const auto connection = SegmentConnection::connect(segment_name(topic), message_of_size(size),
                                                     IndexRing::kMaxCapacity);
  for ([[maybe_unused]] auto _ : state) {
    if (!publish_one(publisher)) {
      state.SkipWithError("no free chunk");
      break;
    }
    take_and_finish(*connection);
  }
}
BENCHMARK(publish_and_take)->Arg(64)->Arg(4 << 10)->Arg(64 << 10)->Arg(1 << 20);

// What a copying transport pays at least once per message, for the same sizes.
void copy(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  std::vector<std::byte> source(size, std::byte{1});
  std::vector<std::byte> destination(size);
  for ([[maybe_unused]] auto _ : state) {
    std::memcpy(destination.data(), source.data(), size);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations() * size));
}
BENCHMARK(copy)->Arg(64)->Arg(4 << 10)->Arg(64 << 10)->Arg(1 << 20);

// One publish delivered to N subscribers, each of which then takes and finishes it.
void publish_fan_out(benchmark::State& state) {
  const auto subscribers = static_cast<std::size_t>(state.range(0));
  const std::string topic = unique_topic();
  ShmPublisher publisher(topic, message_of_size(64));
  std::vector<std::unique_ptr<SegmentConnection>> connections;
  for (std::size_t i = 0; i < subscribers; ++i) {
    connections.push_back(SegmentConnection::connect(segment_name(topic), message_of_size(64),
                                                     IndexRing::kMaxCapacity));
  }
  for ([[maybe_unused]] auto _ : state) {
    if (!publish_one(publisher)) {
      state.SkipWithError("no free chunk");
      break;
    }
    for (const auto& connection : connections) {
      take_and_finish(*connection);
    }
  }
}
BENCHMARK(publish_fan_out)->Arg(1)->Arg(2)->Arg(4)->Arg(kMaxSubscribers);

// The price a publish pays per subscriber to find out it need not wake anyone.
void notify_with_nobody_asleep(benchmark::State& state) {
  WakeSignal signal;
  for ([[maybe_unused]] auto _ : state) {
    signal.notify();
  }
}
BENCHMARK(notify_with_nobody_asleep);

}  // namespace
}  // namespace rtbus::detail
