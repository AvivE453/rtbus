#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>

#include "rtbus/detail/topic.hpp"

namespace rtbus::detail {

// The process-wide map from topic name to topic. Topics are created on first use and
// live until the process exits, so publishers and subscribers can come and go freely.
class TopicRegistry {
 public:
  static TopicRegistry& instance();

  // Throws std::invalid_argument if `name` already exists with a different message type.
  template <typename T>
  std::shared_ptr<Topic<T>> topic(const std::string& name) {
    auto topic = find_or_create(name, typeid(T), [] { return std::make_shared<Topic<T>>(); });
    return std::static_pointer_cast<Topic<T>>(topic);
  }

 private:
  TopicRegistry() = default;

  std::shared_ptr<TopicBase> find_or_create(
      const std::string& name, std::type_index message_type,
      const std::function<std::shared_ptr<TopicBase>()>& make_topic);

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<TopicBase>> topics_;
};

}  // namespace rtbus::detail
