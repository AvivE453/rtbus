#include "rtbus/node.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "collector.hpp"

namespace rtbus {
namespace {

using test_support::Collector;

// The topic registry is process-wide and --gtest_repeat reruns tests in one process,
// so every test uses fresh topic names.
std::string unique_topic(const std::string& base) {
  static std::atomic<int> counter{0};
  return base + "_" + std::to_string(counter++);
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

  publisher.publish(Pose{1.5, -2.0, 7});

  const auto received = collector.wait_until_count(1);
  EXPECT_EQ(received[0].x, 1.5);
  EXPECT_EQ(received[0].y, -2.0);
  EXPECT_EQ(received[0].sequence, 7u);
}

TEST(NodeTest, EverySubscriberReceivesEveryMessage) {
  constexpr int kMessages = 100;
  const auto topic = unique_topic("fanout");
  Node node("node");
  std::vector<Collector<int>> collectors(3);
  std::vector<Subscriber<int>> subscribers;
  subscribers.reserve(collectors.size());
  for (auto& collector : collectors) {
    subscribers.push_back(
        node.subscribe<int>(topic, [&collector](const int& v) { collector.add(v); }, kMessages));
  }
  auto publisher = node.advertise<int>(topic);

  std::vector<int> expected;
  for (int i = 0; i < kMessages; ++i) {
    publisher.publish(i);
    expected.push_back(i);
  }

  for (auto& collector : collectors) {
    EXPECT_EQ(collector.wait_until_count(kMessages), expected);
  }
}

TEST(NodeTest, TopicsDoNotLeakIntoEachOther) {
  const auto topic_a = unique_topic("a");
  const auto topic_b = unique_topic("b");
  Node node("node");
  Collector<int> collector_b;
  auto subscriber_b = node.subscribe<int>(topic_b, [&](const int& v) { collector_b.add(v); });

  node.advertise<int>(topic_a).publish(1);
  node.advertise<int>(topic_b).publish(2);

  EXPECT_EQ(collector_b.wait_until_count(1), std::vector<int>{2});
}

TEST(NodeTest, SameTopicWithDifferentTypeIsRejected) {
  const auto topic = unique_topic("typed");
  Node node("node");
  auto publisher = node.advertise<int>(topic);

  EXPECT_THROW(static_cast<void>(node.advertise<Pose>(topic)), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(node.subscribe<Pose>(topic, [](const Pose&) {})),
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
  Collector<int> stays;
  auto staying = node.subscribe<int>(topic, [&](const int& v) { stays.add(v); });
  std::atomic<int> left_count{0};
  auto publisher = node.advertise<int>(topic);
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
  Collector<int> collector;
  auto original = node.subscribe<int>(topic, [&](const int& v) { collector.add(v); });

  Subscriber<int> moved = std::move(original);
  node.advertise<int>(topic).publish(5);

  EXPECT_EQ(collector.wait_until_count(1), std::vector<int>{5});
}

// Under the tsan and asan presets this is the check for the dangerous race: a subscriber
// being destroyed while a publisher is delivering to it on another thread.
TEST(NodeTest, SubscribersComeAndGoWhilePublishing) {
  const auto topic = unique_topic("churn");
  Node node("node");
  std::atomic<bool> stop{false};
  std::thread publisher_thread([&] {
    auto publisher = node.advertise<int>(topic);
    for (int i = 0; !stop.load(); ++i) {
      publisher.publish(i);
    }
  });

  for (int round = 0; round < 200; ++round) {
    std::atomic<int> received{0};
    auto subscriber = node.subscribe<int>(topic, [&](const int&) { ++received; }, 4);
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
          8));
    }
  }

  std::vector<std::thread> publishers;
  publishers.reserve(topics.size());
  for (const auto& topic : topics) {
    publishers.emplace_back([&node, topic] {
      auto publisher = node.advertise<std::uint64_t>(topic);
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
