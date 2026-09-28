#pragma once

#include <cstdint>
#include <new>
#include <utility>

#include "rtbus/detail/shm_publisher.hpp"

namespace rtbus {

template <typename T>
class Publisher;

// A message in shared memory, lent to the caller to fill in place and then hand to
// Publisher::publish(). Nothing is copied on the way to subscribers.
//
// The message is default-initialized, not zeroed: fields without a default member
// initializer hold whatever an earlier message left there until written.
//
// Destroying a Loan without publishing it returns the memory. A Loan must not outlive the
// Publisher it came from. Move-only.
template <typename T>
class Loan {
 public:
  ~Loan() { give_back(); }

  Loan(const Loan&) = delete;
  Loan& operator=(const Loan&) = delete;

  Loan(Loan&& other) noexcept
      : publisher_(std::exchange(other.publisher_, nullptr)),
        chunk_(other.chunk_),
        message_(other.message_) {}

  Loan& operator=(Loan&& other) noexcept {
    if (this != &other) {
      give_back();
      publisher_ = std::exchange(other.publisher_, nullptr);
      chunk_ = other.chunk_;
      message_ = other.message_;
    }
    return *this;
  }

  T& operator*() const { return *message_; }
  T* operator->() const { return message_; }

 private:
  friend class Publisher<T>;

  // Placement new starts the message's lifetime in the chunk; for a plain struct it compiles
  // to nothing.
  Loan(detail::ShmPublisher* publisher, std::uint32_t chunk)
      : publisher_(publisher), chunk_(chunk), message_(new(publisher->payload(chunk)) T) {}

  void give_back() {
    if (publisher_ != nullptr) {
      publisher_->discard(chunk_);
      publisher_ = nullptr;
    }
  }

  detail::ShmPublisher* publisher_;
  std::uint32_t chunk_;
  T* message_;
};

}  // namespace rtbus
