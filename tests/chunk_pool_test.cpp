#include "rtbus/detail/chunk_pool.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <optional>
#include <thread>

#include "rtbus/detail/index_ring.hpp"
#include "rtbus/shared_memory_region.hpp"
#include "unique_shm_name.hpp"

namespace rtbus::detail {
namespace {

using test_support::unique_shm_name;

SharedMemoryRegion region_for(std::uint32_t chunk_count, std::size_t payload_size) {
  return SharedMemoryRegion::create(unique_shm_name(),
                                    ChunkPool::bytes_needed(chunk_count, payload_size));
}

TEST(ChunkPoolTest, AllocateHandsOutEveryChunkOnceThenRunsDry) {
  auto region = region_for(3, 64);
  ChunkPool pool = ChunkPool::create(region.data(), 3, 64);

  EXPECT_EQ(pool.allocate(), 0u);
  EXPECT_EQ(pool.allocate(), 1u);
  EXPECT_EQ(pool.allocate(), 2u);
  EXPECT_EQ(pool.allocate(), std::nullopt);
}

TEST(ChunkPoolTest, ChunkIsFreeAgainOnlyAfterItsLastRelease) {
  auto region = region_for(1, 64);
  ChunkPool pool = ChunkPool::create(region.data(), 1, 64);
  constexpr std::uint32_t kIndex = 0;
  ASSERT_EQ(pool.allocate(), kIndex);
  pool.retain(kIndex);

  pool.release(kIndex);
  EXPECT_EQ(pool.allocate(), std::nullopt);

  pool.release(kIndex);
  EXPECT_EQ(pool.allocate(), kIndex);
}

TEST(ChunkPoolTest, PayloadsAreAlignedAndDoNotOverlap) {
  constexpr std::uint32_t kChunks = 4;
  constexpr std::size_t kPayloadSize = 100;
  auto region = region_for(kChunks, kPayloadSize);
  ChunkPool pool = ChunkPool::create(region.data(), kChunks, kPayloadSize);

  for (std::uint32_t i = 0; i < kChunks; ++i) {
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(pool.payload(i)) % ChunkPool::kAlignment, 0u);
    std::memset(pool.payload(i), static_cast<int>(i + 1), kPayloadSize);
  }
  for (std::uint32_t i = 0; i < kChunks; ++i) {
    const auto* bytes = static_cast<const std::uint8_t*>(pool.payload(i));
    EXPECT_TRUE(
        std::all_of(bytes, bytes + kPayloadSize, [i](std::uint8_t b) { return b == i + 1; }))
        << "chunk " << i << " was overwritten by a neighbour";
  }
}

// Two mappings of the same memory, as two processes would have: the chunks, their payloads
// and their reference counts must be the same through both addresses.
TEST(ChunkPoolTest, AttachedViewAtAnotherAddressSharesTheChunks) {
  auto region = region_for(2, 64);
  auto second_mapping = SharedMemoryRegion::open(region.name());
  ASSERT_NE(region.data(), second_mapping.data());
  ChunkPool creator = ChunkPool::create(region.data(), 2, 64);
  ChunkPool attached = ChunkPool::attach(second_mapping.data(), 2, 64);

  constexpr std::uint32_t kIndex = 0;
  ASSERT_EQ(creator.allocate(), kIndex);
  *static_cast<std::uint32_t*>(creator.payload(kIndex)) = 42;
  EXPECT_EQ(*static_cast<const std::uint32_t*>(attached.payload(kIndex)), 42u);

  attached.release(kIndex);
  EXPECT_EQ(creator.allocate(), kIndex);
}

// Every word holds the message's sequence number, so a torn or stale read shows up as
// a message whose words disagree.
struct Message {
  std::uint64_t words[32];
};

// The full hand-off that the publisher and a subscriber will perform, on two threads. The
// payload is written and read with ordinary, non-atomic accesses, so under the tsan preset
// any gap in the release/acquire chain (IndexRing for the hand-over, ChunkPool for the reuse)
// is reported as a data race on the payload.
TEST(ChunkPoolTest, PublisherAndSubscriberShareChunksWithoutRaces) {
  constexpr std::uint64_t kMessages = 100'000;
  constexpr std::uint32_t kQueueCapacity = 4;
  // The sizing rule: the chunks the queue can hold, the one the subscriber is reading, and
  // the one the publisher is filling. If the rule were wrong, allocate() would run dry.
  constexpr std::uint32_t kChunks = kQueueCapacity + 1 + 1;
  auto region = region_for(kChunks, sizeof(Message));
  ChunkPool pool = ChunkPool::create(region.data(), kChunks, sizeof(Message));
  IndexRing ring(kQueueCapacity);
  std::atomic<bool> publisher_gave_up{false};
  bool payloads_intact = true;
  bool in_order = true;

  std::thread subscriber([&] {
    std::uint64_t last = 0;
    while (last != kMessages && !publisher_gave_up.load()) {
      const std::optional<std::uint32_t> index = ring.pop();
      if (!index) {
        std::this_thread::yield();
        continue;
      }
      const auto& message = *static_cast<const Message*>(pool.payload(*index));
      const std::uint64_t sequence = message.words[0];
      payloads_intact =
          payloads_intact && std::all_of(std::begin(message.words), std::end(message.words),
                                         [&](std::uint64_t w) { return w == sequence; });
      in_order = in_order && sequence > last;
      last = sequence;
      pool.release(*index);
    }
  });

  bool pool_ran_dry = false;
  for (std::uint64_t sequence = 1; sequence <= kMessages; ++sequence) {
    const std::optional<std::uint32_t> index = pool.allocate();
    if (!index) {
      pool_ran_dry = true;
      publisher_gave_up.store(true);
      break;
    }
    auto& message = *static_cast<Message*>(pool.payload(*index));
    std::fill(std::begin(message.words), std::end(message.words), sequence);
    // The queue's reference is added before the push: once pushed, the subscriber may pop
    // and release it at once, and the count must not reach zero while we still write.
    pool.retain(*index);
    if (const std::optional<std::uint32_t> evicted = ring.push(*index)) {
      pool.release(*evicted);
    }
    pool.release(*index);
  }
  subscriber.join();

  ASSERT_FALSE(pool_ran_dry) << "the sizing rule left too few chunks";
  EXPECT_TRUE(payloads_intact);
  EXPECT_TRUE(in_order);
  // The subscriber stopped on the last message, so the queue is empty and every reference
  // is back: a leaked or doubly released reference would leave a chunk unallocatable.
  for (std::uint32_t i = 0; i < kChunks; ++i) {
    EXPECT_NE(pool.allocate(), std::nullopt) << "a reference was leaked or released twice";
  }
}

}  // namespace
}  // namespace rtbus::detail
