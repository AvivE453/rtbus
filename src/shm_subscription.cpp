#include "rtbus/detail/shm_subscription.hpp"

#include <chrono>
#include <exception>
#include <optional>
#include <stdexcept>
#include <utility>

#include "rtbus/detail/index_ring.hpp"
#include "rtbus/detail/segment.hpp"

namespace rtbus::detail {
namespace {

using Clock = std::chrono::steady_clock;

// How long to wait before trying again to connect while the topic has no usable publisher.
constexpr auto kRetryInterval = std::chrono::milliseconds(10);
// How often a connected subscriber checks that its publisher is still alive. A publisher that
// shuts down normally wakes its subscribers at once; only a crashed one takes this long to
// notice.
constexpr auto kLivenessInterval = std::chrono::milliseconds(100);

// SegmentConnection::connect(), but nullptr instead of an exception. On this thread nobody
// could receive the error: a publisher of another message type, or a topic whose slots all
// belong to live subscribers, is waited out like a missing publisher.
std::unique_ptr<SegmentConnection> connect_or_null(const std::string& segment_name,
                                                   const MessageType& type,
                                                   std::uint32_t capacity) {
  try {
    return SegmentConnection::connect(segment_name, type, capacity);
  } catch (const std::exception&) {
    return nullptr;
  }
}

void pause_in_spin_loop() {
#if defined(__x86_64__) || defined(__i386__)
  // Tells the CPU this is a spin loop, which saves power and leaves execution resources to
  // the other hardware thread on the same core.
  __builtin_ia32_pause();
#endif
}

}  // namespace

ShmSubscription::ShmSubscription(const std::string& topic, const MessageType& type,
                                 const SubscriberOptions& options, Callback callback)
    : segment_name_(segment_name(topic)),
      type_(type),
      options_(options),
      callback_(std::move(callback)) {
  if (options.queue_capacity == 0 || options.queue_capacity > IndexRing::kMaxCapacity) {
    throw std::invalid_argument("queue_capacity must be between 1 and " +
                                std::to_string(IndexRing::kMaxCapacity));
  }
  // Connecting before the thread exists lets a type mismatch reach the caller as an
  // exception, and a subscriber created after its publisher gets the very next message.
  connection_ = SegmentConnection::connect(segment_name_, type_, options_.queue_capacity);
  thread_ = std::thread([this] { run(); });
}

ShmSubscription::~ShmSubscription() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_.store(true);
    if (connection_ != nullptr) {
      connection_->wake();
    }
  }
  retry_signal_.notify();
  thread_.join();
}

std::uint64_t ShmSubscription::dropped() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return dropped_before_ + (connection_ != nullptr ? connection_->dropped() : 0);
}

void ShmSubscription::run() {
  while (!stopping_.load()) {
    if (connection_ == nullptr) {
      try_to_connect();
    } else {
      receive_until_disconnected(*connection_);
      set_connection(nullptr);
    }
  }
}

void ShmSubscription::try_to_connect() {
  std::unique_ptr<SegmentConnection> connection =
      connect_or_null(segment_name_, type_, options_.queue_capacity);
  if (connection != nullptr) {
    set_connection(std::move(connection));
  } else {
    wait_before_retrying();
  }
}

// Returns when the subscription stops or the publisher goes away. Messages already queued
// are delivered first, even from a publisher that has closed.
void ShmSubscription::receive_until_disconnected(SegmentConnection& connection) {
  auto next_liveness_check = Clock::now() + kLivenessInterval;
  while (!stopping_.load()) {
    if (deliver_next(connection)) {
      continue;
    }
    bool publisher_done = connection.closed();
    if (!publisher_done && Clock::now() >= next_liveness_check) {
      publisher_done = connection.publisher_gone();
      next_liveness_check = Clock::now() + kLivenessInterval;
    }
    if (publisher_done) {
      // The queue was empty a moment ago, but the publisher may have delivered its last
      // messages and closed in between. It closes only after its last delivery, so what is
      // queued now is everything it will ever send.
      while (!stopping_.load() && deliver_next(connection)) {
      }
      return;
    }
    if (options_.wait_mode == WaitMode::kBusyPoll) {
      pause_in_spin_loop();
    } else {
      connection.wait(stopping_, kLivenessInterval);
    }
  }
}

bool ShmSubscription::deliver_next(SegmentConnection& connection) {
  const std::optional<std::uint32_t> chunk = connection.take();
  if (!chunk) {
    return false;
  }
  callback_(connection.payload(*chunk));
  connection.finish(*chunk);
  return true;
}

void ShmSubscription::wait_before_retrying() {
  // The WakeSignal protocol, with `stopping_` as the condition.
  const std::uint32_t ticket = retry_signal_.prepare_wait();
  if (stopping_.load()) {
    retry_signal_.cancel_wait();
    return;
  }
  retry_signal_.wait(ticket, kRetryInterval);
}

void ShmSubscription::set_connection(std::unique_ptr<SegmentConnection> connection) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (connection_ != nullptr) {
    dropped_before_ += connection_->dropped();
  }
  connection_ = std::move(connection);
}

}  // namespace rtbus::detail
