#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace rtbus::detail {

// Lets one thread sleep until another says "something changed". It can live in shared
// memory and works across processes; the sleeping thread waits in the kernel (futex) and
// uses no CPU.
//
// The waiter re-checks its condition between prepare_wait() and wait(), and the notifier
// makes the condition true before notify():
//
//   const auto ticket = signal.prepare_wait();   |   make_condition_true();
//   if (condition()) {                           |   signal.notify();
//     signal.cancel_wait();                      |
//   } else {                                     |
//     signal.wait(ticket, timeout);              |
//   }                                            |
//
// Without this protocol a notification that lands between the waiter's check and its sleep
// would be lost, and the waiter would sleep through a condition that is already true.
//
// One waiter at a time; any number of notifiers. Trivially destructible, so it can be
// placed in shared memory.
class WakeSignal {
 public:
  [[nodiscard]] std::uint32_t prepare_wait() {
    // Acquire keeps the sleeping_ store below from moving above this load; otherwise the
    // ticket could already include the notification that our own store triggers, and the
    // kernel would put us to sleep on it.
    const std::uint32_t ticket = sequence_.load(std::memory_order_acquire);
    sleeping_.store(1, std::memory_order_relaxed);
    // Paired with the fence in notify(). Each side stores (sleeping_ here, the condition
    // there) and then loads what the other side stored. Only seq_cst fences forbid both loads
    // from missing both stores, so either the notifier sees sleeping_ == 1 and wakes us, or
    // our re-check of the condition sees the notifier's change. Acquire/release cannot give
    // this guarantee, because it allows a load to move before an earlier store.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return ticket;
  }

  void cancel_wait() { sleeping_.store(0, std::memory_order_relaxed); }

  // Sleeps until notify(), the timeout, or a spurious wake-up; the caller re-checks its
  // condition in every case. Returns at once if notify() ran since prepare_wait().
  void wait(std::uint32_t ticket, std::chrono::nanoseconds timeout);

  void notify();

 private:
  std::atomic<std::uint32_t> sequence_{0};  // the futex word: changes on every wake-up
  std::atomic<std::uint32_t> sleeping_{0};
};

}  // namespace rtbus::detail
