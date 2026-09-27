#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "rtbus/detail/subscription.hpp"
#include "rtbus/detail/topic.hpp"

namespace rtbus {

class Node;

// Receives messages of type T on one topic, running the callback on a dedicated thread.
// Created by Node::subscribe(). Destroying the Subscriber unsubscribes it and stops its
// thread. Move-only.
template <typename T>
class Subscriber {
 public:
  ~Subscriber() { unsubscribe(); }

  Subscriber(const Subscriber&) = delete;
  Subscriber& operator=(const Subscriber&) = delete;
  Subscriber(Subscriber&&) noexcept = default;

  Subscriber& operator=(Subscriber&& other) noexcept {
    if (this != &other) {
      unsubscribe();
      topic_ = std::move(other.topic_);
      subscription_ = std::move(other.subscription_);
    }
    return *this;
  }

  // Messages overwritten because the queue was full when they arrived.
  [[nodiscard]] std::uint64_t dropped() const { return subscription_->dropped(); }

 private:
  friend class Node;

  Subscriber(std::shared_ptr<detail::Topic<T>> topic, std::size_t capacity,
             typename detail::Subscription<T>::Callback callback)
      : topic_(std::move(topic)),
        subscription_(std::make_unique<detail::Subscription<T>>(capacity, std::move(callback))) {
    topic_->add(subscription_.get());
  }

  void unsubscribe() {
    if (subscription_ == nullptr) {
      return;
    }
    // Order matters: after remove() no publisher can reach the subscription, so it is
    // safe to destroy it (which closes its queue and joins its thread).
    topic_->remove(subscription_.get());
    subscription_.reset();
  }

  std::shared_ptr<detail::Topic<T>> topic_;
  // Held by pointer so the Subscriber can move while the worker thread keeps a stable `this`.
  std::unique_ptr<detail::Subscription<T>> subscription_;
};

}  // namespace rtbus
