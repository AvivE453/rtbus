#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "rtbus/detail/message_type.hpp"
#include "rtbus/detail/segment.hpp"
#include "rtbus/detail/shm_publisher.hpp"
#include "rtbus/loan.hpp"

namespace rtbus {

class Node;

// Sends messages of type T on one topic, through shared memory. Created by Node::advertise().
// A topic has one publisher at a time. Publishing never waits for subscribers: a subscriber
// that falls behind loses its oldest queued messages instead.
// Use a Publisher from one thread at a time. Move-only.
template <typename T>
class Publisher {
 public:
  // Zero-copy publishing: fill the loan in place, then publish(std::move(loan)).
  // Throws std::runtime_error if kMaxLoans (2) loans are already outstanding, or if
  // subscribers that crashed have leaked enough chunks to exhaust the pool.
  [[nodiscard]] Loan<T> loan() {
    const std::optional<std::uint32_t> chunk = core_->loan();
    if (!chunk) {
      if (core_->loans_outstanding() == detail::kMaxLoans) {
        throw std::runtime_error("rtbus: at most " + std::to_string(detail::kMaxLoans) +
                                 " loans may be outstanding per publisher");
      }
      throw std::runtime_error("rtbus: no free chunk; subscribers that crashed leaked some");
    }
    return Loan<T>(core_.get(), *chunk);
  }

  void publish(Loan<T>&& loan) {
    if (loan.publisher_ != core_.get()) {
      throw std::invalid_argument("rtbus: a loan can only be published by its own publisher");
    }
    loan.publisher_ = nullptr;
    core_->publish(loan.chunk_);
  }

  // Copies `message` into a loan and publishes it.
  void publish(const T& message) {
    Loan<T> message_loan = loan();
    *message_loan = message;
    publish(std::move(message_loan));
  }

  // Subscribers connected right now.
  [[nodiscard]] std::size_t subscriber_count() const { return core_->subscriber_count(); }

 private:
  friend class Node;

  explicit Publisher(const std::string& topic)
      : core_(std::make_unique<detail::ShmPublisher>(topic, detail::message_type<T>())) {}

  std::unique_ptr<detail::ShmPublisher> core_;
};

}  // namespace rtbus
