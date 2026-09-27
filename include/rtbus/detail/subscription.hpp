#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>

#include "rtbus/detail/keep_last_queue.hpp"

namespace rtbus::detail {

// One subscriber's delivery machinery: a KeepLastQueue plus a dedicated thread that
// pops messages and runs the callback. deliver() is called from the publishing
// thread and never waits for the callback; the callback always runs on this subscription's
// thread.
//
// Neither copyable nor movable: the worker thread holds `this`.
template <typename T>
class Subscription {
 public:
  using Callback = std::function<void(const T&)>;

  Subscription(std::size_t capacity, Callback callback)
      : queue_(capacity), callback_(std::move(callback)), worker_([this] { run(); }) {}

  ~Subscription() {
    queue_.close();
    worker_.join();
  }

  Subscription(const Subscription&) = delete;
  Subscription& operator=(const Subscription&) = delete;
  Subscription(Subscription&&) = delete;
  Subscription& operator=(Subscription&&) = delete;

  void deliver(const T& message) { queue_.push(message); }

  [[nodiscard]] std::uint64_t dropped() const { return queue_.dropped(); }

 private:
  void run() {
    while (auto message = queue_.pop()) {
      callback_(*message);
    }
  }

  // Declaration order is initialization order: worker_ must come last, because its
  // thread starts running run() immediately and uses queue_ and callback_.
  KeepLastQueue<T> queue_;
  Callback callback_;
  std::thread worker_;
};

}  // namespace rtbus::detail
