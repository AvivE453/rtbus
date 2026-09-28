#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "rtbus/detail/message_type.hpp"
#include "rtbus/detail/segment_connection.hpp"
#include "rtbus/detail/wake_signal.hpp"
#include "rtbus/subscriber_options.hpp"

namespace rtbus::detail {

// A subscriber's thread: it connects to the topic's segment, runs the callback for every
// message, and reconnects when the publisher goes away and a new one appears.
class ShmSubscription {
 public:
  using Callback = std::function<void(const void* payload)>;

  // Connects at once if the topic already has a publisher, and otherwise keeps trying in the
  // background. Throws std::invalid_argument for an invalid topic name or queue capacity, or
  // if the existing publisher carries another message type, and std::runtime_error if the
  // topic already has the maximum number of live subscribers.
  ShmSubscription(const std::string& topic, const MessageType& type,
                  const SubscriberOptions& options, Callback callback);

  // Stops the thread (after the callback in progress, if any) and leaves the segment.
  ~ShmSubscription();

  ShmSubscription(const ShmSubscription&) = delete;
  ShmSubscription& operator=(const ShmSubscription&) = delete;
  ShmSubscription(ShmSubscription&&) = delete;
  ShmSubscription& operator=(ShmSubscription&&) = delete;

  // Messages lost because the queue was full, over every connection so far.
  [[nodiscard]] std::uint64_t dropped() const;

 private:
  void run();
  void try_to_connect();
  void receive_until_disconnected(SegmentConnection& connection);
  // Runs the callback for the next queued message, if there is one.
  bool deliver_next(SegmentConnection& connection);
  void wait_before_retrying();
  void set_connection(std::unique_ptr<SegmentConnection> connection);

  const std::string segment_name_;
  const MessageType type_;
  const SubscriberOptions options_;
  const Callback callback_;

  std::atomic<bool> stopping_{false};
  // Wakes the thread while it waits between connection attempts.
  WakeSignal retry_signal_;

  // Guards replacing connection_ and dropped_before_. The thread reads connection_ without
  // it, since only the thread replaces it; other threads read it only under the mutex.
  mutable std::mutex mutex_;
  std::unique_ptr<SegmentConnection> connection_;
  std::uint64_t dropped_before_ = 0;

  // Started last, in the constructor body: it uses every member above.
  std::thread thread_;
};

}  // namespace rtbus::detail
