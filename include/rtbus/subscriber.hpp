#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "rtbus/detail/message_type.hpp"
#include "rtbus/detail/shm_subscription.hpp"
#include "rtbus/subscriber_options.hpp"

namespace rtbus {

class Node;

// Receives messages of type T on one topic, running the callback on a dedicated thread. The
// message the callback sees lives in shared memory and is valid only during the call.
// Created by Node::subscribe(). Destroying the Subscriber unsubscribes it and stops its
// thread. Move-only.
template <typename T>
class Subscriber {
 public:
  // Messages overwritten because the queue was full when they arrived.
  [[nodiscard]] std::uint64_t dropped() const { return subscription_->dropped(); }

 private:
  friend class Node;

  Subscriber(const std::string& topic, std::function<void(const T&)> callback,
             const SubscriberOptions& options)
      : subscription_(std::make_unique<detail::ShmSubscription>(
            topic, detail::message_type<T>(), options,
            [callback = std::move(callback)](const void* payload) {
              callback(*static_cast<const T*>(payload));
            })) {}

  std::unique_ptr<detail::ShmSubscription> subscription_;
};

}  // namespace rtbus
