#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "doomed_child.hpp"
#include "rtbus/detail/segment_connection.hpp"
#include "rtbus/detail/shm_publisher.hpp"
#include "rtbus/file_descriptor.hpp"

namespace rtbus::detail {
namespace {

// Topics are machine-wide names in /dev/shm, so the pid keeps parallel test runs apart.
std::string unique_topic() {
  static int counter = 0;
  return "transport_test_" + std::to_string(::getpid()) + "_" + std::to_string(counter++);
}

const MessageType int_type = message_type<int>();

std::unique_ptr<SegmentConnection> connect(const std::string& topic, std::uint32_t capacity = 4) {
  return SegmentConnection::connect(segment_name(topic), int_type, capacity);
}

void publish(ShmPublisher& publisher, int value) {
  const std::optional<std::uint32_t> chunk = publisher.loan();
  if (!chunk) {
    FAIL() << "no free chunk";
  }
  *static_cast<int*>(publisher.payload(*chunk)) = value;
  publisher.publish(*chunk);
}

// The value of the next message, or std::nullopt if the queue is empty.
std::optional<int> receive(SegmentConnection& connection) {
  const std::optional<std::uint32_t> chunk = connection.take();
  if (!chunk) {
    return std::nullopt;
  }
  const int value = *static_cast<const int*>(connection.payload(*chunk));
  connection.finish(*chunk);
  return value;
}

// True exactly when no reference is leaked, once nobody holds a message.
bool every_chunk_is_free(const ShmPublisher& publisher) {
  return publisher.free_chunk_count() == kChunkCount;
}

TEST(ShmTransportTest, PublishedMessageReachesConnectedSubscriber) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  auto connection = connect(topic);
  ASSERT_NE(connection, nullptr);

  publish(publisher, 42);

  EXPECT_EQ(receive(*connection), 42);
  EXPECT_EQ(receive(*connection), std::nullopt);
}

TEST(ShmTransportTest, EveryConnectedSubscriberReceivesEachMessage) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  auto first = connect(topic);
  auto second = connect(topic);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  publish(publisher, 7);

  EXPECT_EQ(receive(*first), 7);
  EXPECT_EQ(receive(*second), 7);
  EXPECT_TRUE(every_chunk_is_free(publisher));
}

TEST(ShmTransportTest, NothingToConnectToBeforeThePublisherExists) {
  EXPECT_EQ(connect(unique_topic()), nullptr);
}

TEST(ShmTransportTest, SecondLivePublisherOnATopicIsRejected) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);

  EXPECT_THROW(ShmPublisher(topic, int_type), std::runtime_error);
}

TEST(ShmTransportTest, TopicIsFreeAgainAfterItsPublisherIsDestroyed) {
  const auto topic = unique_topic();
  { ShmPublisher first(topic, int_type); }

  ShmPublisher second(topic, int_type);
  auto connection = connect(topic);
  ASSERT_NE(connection, nullptr);
  publish(second, 3);
  EXPECT_EQ(receive(*connection), 3);
}

// A crashed publisher leaves its segment file behind with nobody holding its lock.
TEST(ShmTransportTest, SegmentLeftByDeadPublisherIsReplaced) {
  const auto topic = unique_topic();
  const std::string name = segment_name(topic);
  FileDescriptor leftover(::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600));
  ASSERT_TRUE(leftover.valid());
  ASSERT_EQ(::ftruncate(leftover.get(), 4096), 0);
  leftover.reset();
  EXPECT_EQ(connect(topic), nullptr) << "a subscriber must not attach to a leftover";

  ShmPublisher publisher(topic, int_type);
  auto connection = connect(topic);
  ASSERT_NE(connection, nullptr);
  publish(publisher, 9);
  EXPECT_EQ(receive(*connection), 9);
}

TEST(ShmTransportTest, SubscriberOfAnotherMessageTypeIsRejected) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);

  EXPECT_THROW(
      static_cast<void>(SegmentConnection::connect(segment_name(topic), message_type<double>(), 4)),
      std::invalid_argument);
}

TEST(ShmTransportTest, SlowSubscriberKeepsTheLatestMessagesAndCountsTheRest) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  auto connection = connect(topic, /*capacity=*/2);
  ASSERT_NE(connection, nullptr);

  for (int value = 1; value <= 5; ++value) {
    publish(publisher, value);
  }

  EXPECT_EQ(receive(*connection), 4);
  EXPECT_EQ(receive(*connection), 5);
  EXPECT_EQ(connection->dropped(), 3u);
  EXPECT_TRUE(every_chunk_is_free(publisher));
}

TEST(ShmTransportTest, SubscriberCountFollowsConnections) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  EXPECT_EQ(publisher.subscriber_count(), 0u);

  auto connection = connect(topic);
  EXPECT_EQ(publisher.subscriber_count(), 1u);

  connection.reset();
  EXPECT_EQ(publisher.subscriber_count(), 0u);
}

TEST(ShmTransportTest, SubscriberBeyondTheMaximumIsRejected) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  std::vector<std::unique_ptr<SegmentConnection>> connections;
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    connections.push_back(connect(topic));
    ASSERT_NE(connections.back(), nullptr);
  }

  EXPECT_THROW(static_cast<void>(connect(topic)), std::runtime_error);
}

// A subscriber that leaves with messages still queued: the next publish takes those chunks
// back, and the slot can be claimed again.
TEST(ShmTransportTest, LeftSlotIsReclaimedWithItsQueuedChunks) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  std::vector<std::unique_ptr<SegmentConnection>> connections;
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    connections.push_back(connect(topic));
  }
  publish(publisher, 1);
  publish(publisher, 2);

  connections.back().reset();
  EXPECT_EQ(connect(topic), nullptr) << "the slot is not free until the publisher reclaims it";
  publish(publisher, 3);

  connections.back() = connect(topic);
  ASSERT_NE(connections.back(), nullptr);
  connections.clear();
  publish(publisher, 4);  // reclaims every slot
  EXPECT_TRUE(every_chunk_is_free(publisher));
}

TEST(ShmTransportTest, SubscriberSeesThePublisherClose) {
  const auto topic = unique_topic();
  auto publisher = std::make_unique<ShmPublisher>(topic, int_type);
  auto connection = connect(topic);
  ASSERT_NE(connection, nullptr);
  EXPECT_FALSE(connection->publisher_gone());

  publish(*publisher, 5);
  publisher.reset();

  EXPECT_TRUE(connection->closed());
  EXPECT_TRUE(connection->publisher_gone());
  EXPECT_EQ(receive(*connection), 5) << "messages published before closing are still readable";
}

TEST(ShmTransportTest, WaitReturnsOnceAMessageIsQueued) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  auto connection = connect(topic);
  ASSERT_NE(connection, nullptr);
  const std::atomic<bool> stop{false};
  publish(publisher, 1);

  connection->wait(stop, std::chrono::hours(1));

  EXPECT_EQ(receive(*connection), 1);
}

// kChunkCount is sized for kMaxLoans outstanding loans, so the limit holds even while the
// pool is nearly empty: a caller over it fails at once, not only once subscribers fall behind.
TEST(ShmTransportTest, LoanBeyondTheLimitFailsUntilOneIsReturned) {
  ShmPublisher publisher(unique_topic(), int_type);
  const std::optional<std::uint32_t> first = publisher.loan();
  if (!first) {
    FAIL() << "the first loan must succeed";
  }
  const std::optional<std::uint32_t> second = publisher.loan();
  if (!second) {
    FAIL() << "the second loan must succeed";
  }
  EXPECT_EQ(publisher.loan(), std::nullopt);

  publisher.discard(*first);
  const std::optional<std::uint32_t> third = publisher.loan();
  if (!third) {
    FAIL() << "a discarded loan makes room for another";
  }
  publisher.publish(*third);
  const std::optional<std::uint32_t> fourth = publisher.loan();
  if (!fourth) {
    FAIL() << "a published loan makes room for another";
  }

  publisher.discard(*second);
  publisher.discard(*fourth);
  EXPECT_EQ(publisher.loans_outstanding(), 0u);
  EXPECT_TRUE(every_chunk_is_free(publisher));
}

TEST(ShmTransportTest, SubscriberKilledInItsCallbackCostsThePublisherNothing) {
  const auto topic = unique_topic();
  ShmPublisher publisher(topic, int_type);
  test_support::DoomedChild subscriber([&](test_support::DoomedChild& self) {
    auto connection = connect(topic);
    if (connection == nullptr) {
      return;
    }
    self.report();
    const std::atomic<bool> never{false};
    while (!connection->take()) {
      connection->wait(never, std::chrono::hours(1));
    }
    self.report();  // holding the message, as a callback would
    test_support::wait_to_be_killed();
  });
  ASSERT_TRUE(subscriber.wait_for_report()) << "the subscriber did not connect";
  publish(publisher, 1);
  ASSERT_TRUE(subscriber.wait_for_report()) << "the subscriber did not take the message";
  publish(publisher, 2);
  subscriber.kill();

  // Nobody frees the dead subscriber's chunks yet, but its queue keeps only its latest
  // messages, so publishing never runs out of chunks.
  for (int value = 0; value < 1000; ++value) {
    publish(publisher, value);
  }

  // A newcomer finds the dead subscriber's slot and marks it as left. The next publish takes
  // back its chunks, the one it died holding included.
  auto newcomer = connect(topic);
  ASSERT_NE(newcomer, nullptr);
  publish(publisher, 3);
  EXPECT_EQ(publisher.subscriber_count(), 1u);
  EXPECT_EQ(receive(*newcomer), 3);
  newcomer.reset();
  publish(publisher, 4);
  EXPECT_TRUE(every_chunk_is_free(publisher));
}

}  // namespace
}  // namespace rtbus::detail
