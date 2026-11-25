#pragma once

#include <cstdint>

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
};

}  // namespace rtbus
