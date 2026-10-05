#include "rtbus/detail/index_ring.hpp"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rtbus/shared_memory_region.hpp"
#include "unique_shm_name.hpp"

namespace rtbus::detail {
namespace {

TEST(IndexRingTest, CapacityOutsideRangeIsRejected) {
  EXPECT_THROW(IndexRing(0), std::invalid_argument);
  EXPECT_THROW(IndexRing(IndexRing::kMaxCapacity + 1), std::invalid_argument);
}

TEST(IndexRingTest, PopFromEmptyRingReturnsNothing) {
  IndexRing ring(4);

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(ring.pop(), std::nullopt);
}

TEST(IndexRingTest, EmptyTracksPushesAndPops) {
  IndexRing ring(2);
  EXPECT_EQ(ring.push(1), std::nullopt);
  EXPECT_FALSE(ring.empty());

  EXPECT_EQ(ring.pop(), 1u);
  EXPECT_TRUE(ring.empty());
}

TEST(IndexRingTest, PopReturnsIndicesInFifoOrder) {
  IndexRing ring(4);
  EXPECT_EQ(ring.push(10), std::nullopt);
  EXPECT_EQ(ring.push(11), std::nullopt);
  EXPECT_EQ(ring.push(12), std::nullopt);

  EXPECT_EQ(ring.pop(), 10u);
  EXPECT_EQ(ring.pop(), 11u);
  EXPECT_EQ(ring.pop(), 12u);
  EXPECT_EQ(ring.pop(), std::nullopt);
  EXPECT_EQ(ring.dropped(), 0u);
}

TEST(IndexRingTest, PushIntoFullRingEvictsOldestAndCountsIt) {
  IndexRing ring(3);
  EXPECT_EQ(ring.push(1), std::nullopt);
  EXPECT_EQ(ring.push(2), std::nullopt);
  EXPECT_EQ(ring.push(3), std::nullopt);

  EXPECT_EQ(ring.push(4), 1u);
  EXPECT_EQ(ring.push(5), 2u);

  EXPECT_EQ(ring.pop(), 3u);
  EXPECT_EQ(ring.pop(), 4u);
  EXPECT_EQ(ring.pop(), 5u);
  EXPECT_EQ(ring.dropped(), 2u);
}

TEST(IndexRingTest, SlotsAreReusedAcrossManyWrapArounds) {
  IndexRing ring(3);
  for (std::uint32_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(ring.push(i), std::nullopt);
    ASSERT_EQ(ring.push(i + 1), std::nullopt);
    ASSERT_EQ(ring.pop(), i);
    ASSERT_EQ(ring.pop(), i + 1);
  }
  EXPECT_EQ(ring.pop(), std::nullopt);
}

// Under the tsan preset this is the data-race check for the memory orderings. It also checks
// the ownership rule behind KeepLast: every index is either popped by the consumer or
// returned to the producer as evicted, exactly once, never both and never neither.
// The last index is never evicted (nothing is pushed after it), so the consumer stops on it.
TEST(IndexRingTest, ConcurrentPushAndPopHandOutEveryIndexExactlyOnce) {
  constexpr std::uint32_t kIndices = 100'000;
  IndexRing ring(4);
  std::vector<std::uint8_t> popped(kIndices + 1, 0);
  std::vector<std::uint8_t> evicted(kIndices + 1, 0);
  bool in_order = true;

  std::thread consumer([&] {
    std::uint32_t last = 0;
    while (last != kIndices) {
      const std::optional<std::uint32_t> index = ring.pop();
      if (!index) {
        std::this_thread::yield();
        continue;
      }
      in_order = in_order && *index > last;
      last = *index;
      ++popped[*index];
    }
  });
  for (std::uint32_t i = 1; i <= kIndices; ++i) {
    if (const std::optional<std::uint32_t> oldest = ring.push(i)) {
      ++evicted[*oldest];
    }
  }
  consumer.join();

  EXPECT_TRUE(in_order);
  std::uint64_t evicted_count = 0;
  for (std::uint32_t i = 1; i <= kIndices; ++i) {
    ASSERT_EQ(popped[i] + evicted[i], 1) << "index " << i;
    evicted_count += evicted[i];
  }
  EXPECT_EQ(ring.dropped(), evicted_count);
}

// The ring and the test's own bookkeeping, laid out together in one shared-memory region.
struct CrossProcessState {
  IndexRing ring{8};
  std::atomic<bool> consumer_ready{false};
  std::atomic<std::uint64_t> popped{0};
};

// Same contract as above, but between two processes. The child opens the region by name,
// so it sees the ring at a different address than the parent: this is where storing
// pointers instead of indices would break.
TEST(IndexRingTest, ProducerAndConsumerInSeparateProcesses) {
  constexpr std::uint32_t kIndices = 100'000;
  const std::string name = test_support::unique_shm_name();
  auto region = SharedMemoryRegion::create(name, sizeof(CrossProcessState));
  auto* state = new (region.data()) CrossProcessState;

  const pid_t pid = ::fork();
  ASSERT_NE(pid, -1);
  if (pid == 0) {
    // Leave only via _exit, as in the SharedMemoryRegion fork test.
    int exit_code = 0;
    try {
      auto view = SharedMemoryRegion::open(name);
      auto* shared = static_cast<CrossProcessState*>(view.data());
      shared->consumer_ready.store(true);
      std::uint32_t last = 0;
      while (last != kIndices) {
        if (const std::optional<std::uint32_t> index = shared->ring.pop()) {
          exit_code = *index > last ? exit_code : 1;
          last = *index;
          shared->popped.fetch_add(1);
        }
      }
    } catch (...) {
      exit_code = 2;
    }
    ::_exit(exit_code);
  }

  while (!state->consumer_ready.load()) {
    std::this_thread::yield();
  }
  std::uint64_t evicted = 0;
  for (std::uint32_t i = 1; i <= kIndices; ++i) {
    evicted += state->ring.push(i).has_value() ? 1 : 0;
  }

  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0) << "child popped indices out of order";
  EXPECT_EQ(state->popped.load() + evicted, kIndices);
  EXPECT_EQ(state->ring.dropped(), evicted);
}

}  // namespace
}  // namespace rtbus::detail
