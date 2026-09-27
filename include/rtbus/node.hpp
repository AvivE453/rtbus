#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <utility>

#include "rtbus/detail/topic_registry.hpp"
#include "rtbus/publisher.hpp"
#include "rtbus/subscriber.hpp"

namespace rtbus {

// One named participant in the system. Nodes in the same process find each other's
// topics by name; the name itself will identify the node in discovery (phase 3).
class Node {
 public:
  static constexpr std::size_t kDefaultQueueCapacity = 16;

  explicit Node(std::string name) : name_(std::move(name)) {}

  [[nodiscard]] const std::string& name() const { return name_; }

  template <typename T>
  [[nodiscard]] Publisher<T> advertise(const std::string& topic) {
    return Publisher<T>(detail::TopicRegistry::instance().topic<T>(topic));
  }

  // The callback runs on the subscriber's own thread. When `queue_capacity` messages are
  // waiting, a new message overwrites the oldest one (see Subscriber::dropped()).
  template <typename T>
  [[nodiscard]] Subscriber<T> subscribe(const std::string& topic,
                                        std::function<void(const T&)> callback,
                                        std::size_t queue_capacity = kDefaultQueueCapacity) {
    return Subscriber<T>(detail::TopicRegistry::instance().topic<T>(topic), queue_capacity,
                         std::move(callback));
  }

 private:
  std::string name_;
};

}  // namespace rtbus
