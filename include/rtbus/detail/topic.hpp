#pragma once

#include <algorithm>
#include <mutex>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <vector>

#include "rtbus/detail/subscription.hpp"

namespace rtbus::detail {

// The registry stores topics of every message type side by side, so it holds them
// through this type-erased base and uses message_type() to check casts back to Topic<T>.
class TopicBase {
 public:
  explicit TopicBase(std::type_index message_type) : message_type_(message_type) {}
  virtual ~TopicBase() = default;

  TopicBase(const TopicBase&) = delete;
  TopicBase& operator=(const TopicBase&) = delete;
  TopicBase(TopicBase&&) = delete;
  TopicBase& operator=(TopicBase&&) = delete;

  [[nodiscard]] std::type_index message_type() const { return message_type_; }

 private:
  std::type_index message_type_;
};

template <typename T>
class Topic : public TopicBase {
  static_assert(std::is_trivially_copyable_v<T>,
                "rtbus messages must be trivially copyable: plain structs, with no "
                "std::string, std::vector or other types that own memory");
  static_assert(std::is_default_constructible_v<T>,
                "rtbus messages must be default constructible (queue slots are preallocated)");

 public:
  Topic() : TopicBase(typeid(T)) {}

  void add(Subscription<T>* subscription) {
    std::lock_guard<std::mutex> lock(mutex_);
    subscriptions_.push_back(subscription);
  }

  // Once remove() returns, no publish() is delivering to `subscription`, so the caller
  // may destroy it: publish() holds the same mutex for the whole delivery loop.
  void remove(Subscription<T>* subscription) {
    std::lock_guard<std::mutex> lock(mutex_);
    subscriptions_.erase(std::remove(subscriptions_.begin(), subscriptions_.end(), subscription),
                         subscriptions_.end());
  }

  void publish(const T& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (Subscription<T>* subscription : subscriptions_) {
      subscription->deliver(message);
    }
  }

 private:
  std::mutex mutex_;
  std::vector<Subscription<T>*> subscriptions_;
};

}  // namespace rtbus::detail
