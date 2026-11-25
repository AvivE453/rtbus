#pragma once

#include <functional>
#include <string>
#include <utility>

#include "rtbus/publisher.hpp"
#include "rtbus/subscriber.hpp"
#include "rtbus/subscriber_options.hpp"

namespace rtbus {

// One named participant in the system. Topics are shared by every process on the machine:
// each topic is a shared-memory segment named after it. The node's name will identify it in
// discovery (phase 3).
class Node {
 public:
  explicit Node(std::string name) : name_(std::move(name)) {}

  [[nodiscard]] const std::string& name() const { return name_; }

  // Throws std::invalid_argument for a topic name that is not 1 to 200 characters of
  // [A-Za-z0-9_.-], and std::runtime_error if the topic already has a live publisher.
  template <typename T>
  [[nodiscard]] Publisher<T> advertise(const std::string& topic) {
    return Publisher<T>(topic);
  }

  // The callback runs on the subscriber's own thread. The subscriber connects to the topic's
  // publisher as soon as one exists, and again whenever it is replaced; messages published
  // while it is not connected are not delivered to it.
  // Throws std::invalid_argument if the topic's current publisher carries another message
  // type, and std::runtime_error if the topic already has its maximum of 8 subscribers.
  template <typename T>
  [[nodiscard]] Subscriber<T> subscribe(const std::string& topic,
                                        std::function<void(const T&)> callback,
                                        const SubscriberOptions& options = {}) {
    return Subscriber<T>(topic, std::move(callback), options);
  }

 private:
  std::string name_;
};

}  // namespace rtbus
