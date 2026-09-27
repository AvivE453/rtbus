#include "rtbus/detail/subscription.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "collector.hpp"

namespace rtbus::detail {
namespace {

constexpr auto kTimeout = std::chrono::seconds(5);

using Collector = test_support::Collector<int>;

TEST(SubscriptionTest, CallbackReceivesDeliveredMessagesInOrder) {
  Collector collector;
  Subscription<int> subscription(16, [&](const int& value) { collector.add(value); });

  subscription.deliver(1);
  subscription.deliver(2);
  subscription.deliver(3);

  EXPECT_EQ(collector.wait_until_count(3), (std::vector<int>{1, 2, 3}));
}

TEST(SubscriptionTest, CallbackRunsOnItsOwnThread) {
  std::promise<std::thread::id> callback_thread;
  Subscription<int> subscription(
      16, [&](const int&) { callback_thread.set_value(std::this_thread::get_id()); });

  subscription.deliver(1);
  auto result = callback_thread.get_future();

  ASSERT_EQ(result.wait_for(kTimeout), std::future_status::ready);
  EXPECT_NE(result.get(), std::this_thread::get_id());
}

TEST(SubscriptionTest, DestructionWithNoMessagesDoesNotHang) {
  {
    Subscription<int> subscription(16, [](const int&) {});
  }
  SUCCEED();
}

// The callback blocks on the first message, so everything delivered after it piles up
// in a queue of capacity 2: of the next 5 messages, the 3 oldest are dropped.
TEST(SubscriptionTest, SlowCallbackDropsOldestMessages) {
  Collector collector;
  std::promise<void> entered_first_callback;
  std::promise<void> release;
  std::shared_future<void> released = release.get_future().share();

  Subscription<int> subscription(2, [&](const int& value) {
    if (value == 1) {
      entered_first_callback.set_value();
      released.wait();
    }
    collector.add(value);
  });

  subscription.deliver(1);
  ASSERT_EQ(entered_first_callback.get_future().wait_for(kTimeout), std::future_status::ready);
  for (int value = 2; value <= 6; ++value) {
    subscription.deliver(value);
  }
  release.set_value();

  EXPECT_EQ(collector.wait_until_count(3), (std::vector<int>{1, 5, 6}));
  EXPECT_EQ(subscription.dropped(), 3u);
}

}  // namespace
}  // namespace rtbus::detail
