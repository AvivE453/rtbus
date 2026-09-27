#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <vector>

namespace rtbus::detail {

// A fixed-capacity FIFO queue whose producer never waits for the consumer: when full, a push
// overwrites the oldest message and counts it as dropped. All storage is allocated
// in the constructor, so push() and pop() never allocate.
template <typename T>
class KeepLastQueue {
 public:
  explicit KeepLastQueue(std::size_t capacity) : slots_(capacity) {
    if (capacity == 0) {
      throw std::invalid_argument("KeepLastQueue capacity must be at least 1");
    }
  }

  void push(const T& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    // When the queue is full, tail == head_: the write lands on the oldest message.
    const std::size_t tail = (head_ + size_) % slots_.size();
    slots_[tail] = message;
    if (size_ == slots_.size()) {
      head_ = (head_ + 1) % slots_.size();
      ++dropped_;
    } else {
      ++size_;
    }
    not_empty_.notify_one();
  }

  // Blocks until a message is available. Returns std::nullopt once close() has been
  // called, discarding anything still queued.
  std::optional<T> pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    // The predicate is re-checked after every wake-up, because condition variables
    // can wake without a notify (spurious wake-up).
    not_empty_.wait(lock, [this] { return size_ > 0 || closed_; });
    if (closed_) {
      return std::nullopt;
    }
    T message = slots_[head_];
    head_ = (head_ + 1) % slots_.size();
    --size_;
    return message;
  }

  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    not_empty_.notify_all();
  }

  [[nodiscard]] std::uint64_t dropped() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dropped_;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::vector<T> slots_;
  std::size_t head_ = 0;  // index of the oldest message
  std::size_t size_ = 0;
  std::uint64_t dropped_ = 0;
  bool closed_ = false;
};

}  // namespace rtbus::detail
