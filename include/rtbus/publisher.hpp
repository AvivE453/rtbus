#pragma once

#include <memory>
#include <utility>

#include "rtbus/detail/topic.hpp"

namespace rtbus {

class Node;

// Sends messages of type T on one topic. Created by Node::advertise().
// publish() copies the message into every current subscriber's queue. It never waits for a
// subscriber to process messages, only (briefly) for the queue mutexes.
template <typename T>
class Publisher {
 public:
  void publish(const T& message) { topic_->publish(message); }

 private:
  friend class Node;
  explicit Publisher(std::shared_ptr<detail::Topic<T>> topic) : topic_(std::move(topic)) {}

  std::shared_ptr<detail::Topic<T>> topic_;
};

}  // namespace rtbus
