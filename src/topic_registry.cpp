#include "rtbus/detail/topic_registry.hpp"

#include <stdexcept>

namespace rtbus::detail {

TopicRegistry& TopicRegistry::instance() {
  // A function-local static is initialized exactly once, even if several threads
  // call instance() at the same time (guaranteed since C++11).
  static TopicRegistry registry;
  return registry;
}

std::shared_ptr<TopicBase> TopicRegistry::find_or_create(
    const std::string& name, std::type_index message_type,
    const std::function<std::shared_ptr<TopicBase>()>& make_topic) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto [it, inserted] = topics_.try_emplace(name);
  if (inserted) {
    it->second = make_topic();
  } else if (it->second->message_type() != message_type) {
    throw std::invalid_argument("topic '" + name + "' already carries a different message type");
  }
  return it->second;
}

}  // namespace rtbus::detail
