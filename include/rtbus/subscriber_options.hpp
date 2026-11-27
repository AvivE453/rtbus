#pragma once

#include <cstdint>
#include <exception>
#include <functional>

namespace rtbus {

enum class WaitMode : std::uint8_t {
  // The subscriber's thread sleeps in the kernel until a message arrives (futex): no CPU
  // while idle, at the price of a wake-up on every message.
  kSleep,
  // The thread spins on its queue: the lowest latency, but it keeps a CPU core busy.
  kBusyPoll,
};

struct SubscriberOptions {
  // Messages that can wait for the callback, 1 to 16. When the queue is full, a new message
  // replaces the oldest one, which is counted in Subscriber::dropped().
  std::uint32_t queue_capacity = 16;
  WaitMode wait_mode = WaitMode::kSleep;
  // Called when connecting in the background fails with an error, such as a publisher of
  // another message type. The subscriber keeps retrying. Runs on the subscriber's thread,
  // like the message callback, and like it must not throw. Empty by default: errors are not
  // reported.
  std::function<void(const std::exception&)> on_error = nullptr;
};

}  // namespace rtbus
