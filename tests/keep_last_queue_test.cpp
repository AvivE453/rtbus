#include "rtbus/detail/keep_last_queue.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <thread>

namespace rtbus::detail {
namespace {

TEST(KeepLastQueueTest, ZeroCapacityIsRejected) {
  EXPECT_THROW(KeepLastQueue<int>(0), std::invalid_argument);
}

TEST(KeepLastQueueTest, PopReturnsMessagesInFifoOrder) {
  KeepLastQueue<int> queue(4);
  queue.push(1);
  queue.push(2);
  queue.push(3);

  EXPECT_EQ(queue.pop(), 1);
  EXPECT_EQ(queue.pop(), 2);
  EXPECT_EQ(queue.pop(), 3);
  EXPECT_EQ(queue.dropped(), 0u);
}

TEST(KeepLastQueueTest, FullQueueDropsOldestAndCountsIt) {
  KeepLastQueue<int> queue(3);
  for (int i = 1; i <= 5; ++i) {
    queue.push(i);
  }

  EXPECT_EQ(queue.pop(), 3);
  EXPECT_EQ(queue.pop(), 4);
  EXPECT_EQ(queue.pop(), 5);
  EXPECT_EQ(queue.dropped(), 2u);
}

// close() may run before or after the consumer starts waiting; either way pop() must return.
TEST(KeepLastQueueTest, CloseMakesPopReturnNullopt) {
  KeepLastQueue<int> queue(4);
  std::optional<int> result = 42;

  std::thread consumer([&] { result = queue.pop(); });
  queue.close();
  consumer.join();

  EXPECT_EQ(result, std::nullopt);
}

// Under the tsan preset this is the data-race check. The invariant: the last message
// is never dropped (nothing is pushed after it), so once the consumer sees it, every
// message was either received or counted as dropped, and the received ones arrived
// in increasing order.
TEST(KeepLastQueueTest, ConcurrentProducerAndConsumerLoseNothingUncounted) {
  constexpr std::uint64_t kMessages = 100'000;
  KeepLastQueue<std::uint64_t> queue(8);
  std::uint64_t received = 0;
  bool in_order = true;

  std::thread consumer([&] {
    std::uint64_t last = 0;
    while (last != kMessages) {
      const std::uint64_t value = *queue.pop();
      in_order = in_order && value > last;
      last = value;
      ++received;
    }
  });
  for (std::uint64_t i = 1; i <= kMessages; ++i) {
    queue.push(i);
  }
  consumer.join();

  EXPECT_TRUE(in_order);
  EXPECT_EQ(received + queue.dropped(), kMessages);
}

}  // namespace
}  // namespace rtbus::detail
