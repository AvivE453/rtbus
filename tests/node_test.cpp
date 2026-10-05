#include "rtbus/node.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "collector.hpp"
#include "doomed_child.hpp"

namespace rtbus {
namespace {

using test_support::Collector;

// Topics are machine-wide names in /dev/shm, so the pid keeps parallel test runs apart, and
// the counter keeps --gtest_repeat runs apart.
std::string unique_topic(const std::string& base) {
  static std::atomic<int> counter{0};
  return base + "_" + std::to_string(::getpid()) + "_" + std::to_string(counter++);
}

// A subscriber created before its publisher connects in the background. Tests that publish
// right after advertising first wait until the publisher sees every subscriber.
template <typename T>
void wait_for_subscribers(const Publisher<T>& publisher, std::size_t count) {
  while (publisher.subscriber_count() < count) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

struct Pose {
  double x;
  double y;
  std::uint32_t sequence;
};

TEST(NodeTest, MessageReachesSubscriberOnAnotherNode) {
  const auto topic = unique_topic("pose");
  Node sender("sender");
  Node receiver("receiver");
  Collector<Pose> collector;
  auto subscriber = receiver.subscribe<Pose>(topic, [&](const Pose& p) { collector.add(p); });
  auto publisher = sender.advertise<Pose>(topic);
  wait_for_subscribers(publisher, 1);

  publisher.publish(Pose{1.5, -2.0, 7});

  const auto received = collector.wait_until_count(1);
  EXPECT_EQ(received[0].x, 1.5);
  EXPECT_EQ(received[0].y, -2.0);
  EXPECT_EQ(received[0].sequence, 7u);
}

TEST(NodeTest, SubscriberCreatedAfterItsPublisherIsConnectedAtOnce) {
  const auto topic = unique_topic("late");
  Node node("node");
  auto publisher = node.advertise<int>(topic);
  Collector<int> collector;
  auto subscriber = node.subscribe<int>(topic, [&](const int& v) { collector.add(v); });

  publisher.publish(3);

  EXPECT_EQ(collector.wait_until_count(1), std::vector<int>{3});
}

TEST(NodeTest, EverySubscriberReceivesEveryMessage) {
  constexpr int kMessages = 16;  // the queue capacity, so nothing can be dropped
  const auto topic = unique_topic("fanout");
  Node node("node");
  std::vector<Collector<int>> collectors(3);
  std::vector<Subscriber<int>> subscribers;
  subscribers.reserve(collectors.size());
  for (auto& collector : collectors) {
    subscribers.push_back(
        node.subscribe<int>(topic, [&collector](const int& v) { collector.add(v); }));
  }
  auto publisher = node.advertise<int>(topic);
  wait_for_subscribers(publisher, collectors.size());

  std::vector<int> expected;
  for (int i = 0; i < kMessages; ++i) {
    publisher.publish(i);
    expected.push_back(i);
  }

  for (auto& collector : collectors) {
    EXPECT_EQ(collector.wait_until_count(kMessages), expected);
  }
}

TEST(NodeTest, LoanedMessageIsFilledInPlaceAndDelivered) {
  const auto topic = unique_topic("loan");
  Node node("node");
  auto publisher = node.advertise<Pose>(topic);
  Collector<Pose> collector;
  auto subscriber = node.subscribe<Pose>(topic, [&](const Pose& p) { collector.add(p); });

  Loan<Pose> loan = publisher.loan();
  loan->x = 4.0;
  loan->y = 5.0;
  loan->sequence = 6;
  publisher.publish(std::move(loan));

  const auto received = collector.wait_until_count(1);
  EXPECT_EQ(received[0].x, 4.0);
  EXPECT_EQ(received[0].sequence, 6u);
}

// The pool has a fixed number of chunks: loans that were never returned would exhaust it.
TEST(NodeTest, UnpublishedLoanReturnsItsMemory) {
  Node node("node");
  auto publisher = node.advertise<int>(unique_topic("unpublished"));

  for (int i = 0; i < 1000; ++i) {
    Loan<int> loan = publisher.loan();
    *loan = i;
  }
  publisher.publish(1);
  SUCCEED();
}

TEST(NodeTest, ThirdOutstandingLoanIsRefusedEvenWithoutSubscribers) {
  Node node("node");
  auto publisher = node.advertise<int>(unique_topic("loans"));
  Loan<int> first = publisher.loan();
  const Loan<int> second = publisher.loan();

  EXPECT_THROW(static_cast<void>(publisher.loan()), std::runtime_error);
  publisher.publish(std::move(first));
  EXPECT_NO_THROW(static_cast<void>(publisher.loan()));
}

TEST(NodeTest, LoanCanOnlyBePublishedByItsOwnPublisher) {
  Node node("node");
  auto first = node.advertise<int>(unique_topic("owner"));
  auto second = node.advertise<int>(unique_topic("other"));

  EXPECT_THROW(second.publish(first.loan()), std::invalid_argument);
}

TEST(NodeTest, TopicsDoNotLeakIntoEachOther) {
  const auto topic_a = unique_topic("a");
  const auto topic_b = unique_topic("b");
  Node node("node");
  auto publisher_a = node.advertise<int>(topic_a);
  auto publisher_b = node.advertise<int>(topic_b);
  Collector<int> collector_b;
  auto subscriber_b = node.subscribe<int>(topic_b, [&](const int& v) { collector_b.add(v); });

  publisher_a.publish(1);
  publisher_b.publish(2);

  EXPECT_EQ(collector_b.wait_until_count(1), std::vector<int>{2});
  EXPECT_EQ(publisher_a.subscriber_count(), 0u);
}

TEST(NodeTest, SubscriberOfAnotherMessageTypeIsRejected) {
  const auto topic = unique_topic("typed");
  Node node("node");
  auto publisher = node.advertise<int>(topic);

  EXPECT_THROW(static_cast<void>(node.subscribe<Pose>(topic, [](const Pose&) {})),
               std::invalid_argument);
}

// Without a publisher there is nothing to compare types with; the subscriber stays
// disconnected from a publisher of another type instead.
TEST(NodeTest, SubscriberWaitsOutAPublisherOfAnotherMessageType) {
  const auto topic = unique_topic("typed_later");
  Node node("node");
  auto subscriber = node.subscribe<Pose>(topic, [](const Pose&) {});
  auto publisher = node.advertise<int>(topic);

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_EQ(publisher.subscriber_count(), 0u);
}

TEST(NodeTest, SecondPublisherOnATopicIsRejected) {
  const auto topic = unique_topic("single");
  Node node("node");
  auto publisher = node.advertise<int>(topic);

  EXPECT_THROW(static_cast<void>(node.advertise<int>(topic)), std::runtime_error);
}

TEST(NodeTest, InvalidTopicNameIsRejected) {
  Node node("node");

  EXPECT_THROW(static_cast<void>(node.advertise<int>("a/b")), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(node.subscribe<int>("", [](const int&) {})),
               std::invalid_argument);
}

TEST(NodeTest, QueueCapacityOutsideRangeIsRejected) {
  Node node("node");
  const auto topic = unique_topic("capacity");

  EXPECT_THROW(static_cast<void>(node.subscribe<int>(topic, [](const int&) {}, {0})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(node.subscribe<int>(topic, [](const int&) {}, {17})),
               std::invalid_argument);
}

TEST(NodeTest, PublishWithoutSubscribersDoesNothing) {
  Node node("node");
  auto publisher = node.advertise<int>(unique_topic("empty"));

  publisher.publish(1);
  SUCCEED();
}

TEST(NodeTest, DestroyedSubscriberStopsReceivingWhileOthersContinue) {
  const auto topic = unique_topic("leave");
  Node node("node");
  auto publisher = node.advertise<int>(topic);
  Collector<int> stays;
  auto staying = node.subscribe<int>(topic, [&](const int& v) { stays.add(v); });
  std::atomic<int> left_count{0};
  {
    auto leaving = node.subscribe<int>(topic, [&](const int&) { ++left_count; });
    publisher.publish(1);
    stays.wait_until_count(1);
  }
  const int count_at_exit = left_count.load();
  publisher.publish(2);

  EXPECT_EQ(stays.wait_until_count(2), (std::vector<int>{1, 2}));
  EXPECT_EQ(left_count.load(), count_at_exit);
}

TEST(NodeTest, MovedSubscriberKeepsReceiving) {
  const auto topic = unique_topic("move");
  Node node("node");
  auto publisher = node.advertise<int>(topic);
  Collector<int> collector;
  auto original = node.subscribe<int>(topic, [&](const int& v) { collector.add(v); });

  Subscriber<int> moved = std::move(original);
  publisher.publish(5);

  EXPECT_EQ(collector.wait_until_count(1), std::vector<int>{5});
}

TEST(NodeTest, SubscriberReconnectsWhenThePublisherIsReplaced) {
  const auto topic = unique_topic("replaced");
  Node node("node");
  Collector<int> collector;
  auto subscriber = node.subscribe<int>(topic, [&](const int& v) { collector.add(v); });
  {
    auto first = node.advertise<int>(topic);
    wait_for_subscribers(first, 1);
    first.publish(1);
    collector.wait_until_count(1);
  }

  auto second = node.advertise<int>(topic);
  wait_for_subscribers(second, 1);
  second.publish(2);

  EXPECT_EQ(collector.wait_until_count(2), (std::vector<int>{1, 2}));
}

// The same, but the first publisher never gets to close its segment: the subscriber has to
// notice the death itself, and the new publisher has to replace what the dead one left.
TEST(NodeTest, SubscriberReconnectsWhenThePublisherIsKilled) {
  const auto topic = unique_topic("killed");
  test_support::DoomedChild first_publisher([&](test_support::DoomedChild& self) {
    Node node("doomed");
    auto publisher = node.advertise<int>(topic);
    self.report();
    wait_for_subscribers(publisher, 1);
    publisher.publish(1);
    test_support::wait_to_be_killed();
  });
  ASSERT_TRUE(first_publisher.wait_for_report()) << "the first publisher did not advertise";

  Node node("node");
  Collector<int> collector;
  auto subscriber = node.subscribe<int>(topic, [&](const int& v) { collector.add(v); });
  collector.wait_until_count(1);
  first_publisher.kill();

  auto second = node.advertise<int>(topic);
  wait_for_subscribers(second, 1);
  second.publish(2);

  EXPECT_EQ(collector.wait_until_count(2), (std::vector<int>{1, 2}));
}

TEST(NodeTest, BusyPollingSubscriberReceivesMessages) {
  const auto topic = unique_topic("busy");
  Node node("node");
  auto publisher = node.advertise<int>(topic);
  Collector<int> collector;
  auto subscriber =
      node.subscribe<int>(topic, [&](const int& v) { collector.add(v); }, {4, WaitMode::kBusyPoll});

  publisher.publish(1);
  publisher.publish(2);

  EXPECT_EQ(collector.wait_until_count(2), (std::vector<int>{1, 2}));
}

// Under the tsan and asan presets this is the check for the dangerous race: a subscriber
// joining and leaving while a publisher is delivering to its slot on another thread.
TEST(NodeTest, SubscribersComeAndGoWhilePublishing) {
  const auto topic = unique_topic("churn");
  Node node("node");
  auto publisher = node.advertise<int>(topic);
  std::atomic<bool> stop{false};
  std::thread publisher_thread([&] {
    for (int i = 0; !stop.load(); ++i) {
      publisher.publish(i);
    }
  });

  for (int round = 0; round < 200; ++round) {
    std::atomic<int> received{0};
    auto subscriber = node.subscribe<int>(topic, [&](const int&) { ++received; }, {4});
    std::this_thread::yield();
  }
  stop = true;
  publisher_thread.join();
  SUCCEED();
}

// Several topics, each with its own publisher thread and two subscribers, all at once.
// The last message on a topic is never dropped, so once a subscriber has seen it, every
// message was either received or counted as dropped, and received ones stayed in order.
TEST(NodeTest, ConcurrentTopicsLoseNothingUncounted) {
  constexpr std::size_t kTopics = 4;
  constexpr std::size_t kSubscribersPerTopic = 2;
  constexpr std::uint64_t kMessages = 20'000;

  struct Tally {
    std::uint64_t received = 0;
    bool in_order = true;
    std::promise<void> saw_last;
  };

  Node node("node");
  std::vector<Tally> tallies(kTopics * kSubscribersPerTopic);
  std::vector<Subscriber<std::uint64_t>> subscribers;
  std::vector<std::string> topics;
  subscribers.reserve(tallies.size());
  for (std::size_t t = 0; t < kTopics; ++t) {
    topics.push_back(unique_topic("concurrent"));
    for (std::size_t s = 0; s < kSubscribersPerTopic; ++s) {
      Tally& tally = tallies[t * kSubscribersPerTopic + s];
      subscribers.push_back(node.subscribe<std::uint64_t>(
          topics.back(),
          [&tally, last = std::uint64_t{0}](const std::uint64_t& value) mutable {
            tally.in_order = tally.in_order && value > last;
            last = value;
            ++tally.received;
            if (value == kMessages) {
              tally.saw_last.set_value();
            }
          },
          {8}));
    }
  }

  std::vector<std::thread> publishers;
  publishers.reserve(topics.size());
  for (const auto& topic : topics) {
    publishers.emplace_back([&node, topic] {
      auto publisher = node.advertise<std::uint64_t>(topic);
      wait_for_subscribers(publisher, kSubscribersPerTopic);
      for (std::uint64_t i = 1; i <= kMessages; ++i) {
        publisher.publish(i);
      }
    });
  }
  for (auto& publisher : publishers) {
    publisher.join();
  }
  for (auto& tally : tallies) {
    tally.saw_last.get_future().wait();
  }

  for (std::size_t i = 0; i < tallies.size(); ++i) {
    EXPECT_TRUE(tallies[i].in_order) << "subscriber " << i;
    EXPECT_EQ(tallies[i].received + subscribers[i].dropped(), kMessages) << "subscriber " << i;
  }
}

}  // namespace
}  // namespace rtbus
