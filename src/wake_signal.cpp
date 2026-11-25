#include "rtbus/detail/wake_signal.hpp"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <ctime>
#include <type_traits>

namespace rtbus::detail {
namespace {

// The kernel reads and compares the futex word as a plain 32-bit integer.
static_assert(sizeof(std::atomic<std::uint32_t>) == sizeof(std::uint32_t));
static_assert(std::is_trivially_destructible_v<WakeSignal>);

// glibc has no wrapper for futex. The operations are used without FUTEX_PRIVATE_FLAG: the
// private variant is faster but only matches waiters in the same process, and a subscriber
// usually lives in another process than its publisher.
long futex(std::atomic<std::uint32_t>& word, int operation, std::uint32_t value,
           const timespec* timeout) {
  return ::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&word), operation, value, timeout,
                   nullptr, 0);
}

timespec to_timespec(std::chrono::nanoseconds duration) {
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
  return {static_cast<time_t>(seconds.count()), static_cast<long>((duration - seconds).count())};
}

}  // namespace

void WakeSignal::wait(std::uint32_t ticket, std::chrono::nanoseconds timeout) {
  const timespec relative_timeout = to_timespec(timeout);
  // The kernel sleeps only if the word still equals the ticket, checked atomically with
  // joining the wait queue. Every outcome (woken, timed out, EAGAIN because the word already
  // changed, EINTR) sends the caller back to re-check its condition, so the result is unused.
  futex(sequence_, FUTEX_WAIT, ticket, &relative_timeout);
  sleeping_.store(0, std::memory_order_relaxed);
}

void WakeSignal::notify() {
  // See prepare_wait().
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (sleeping_.load(std::memory_order_relaxed) != 0) {
    // Relaxed: the new value only has to differ from the waiter's ticket. The data the waiter
    // wakes up for is ordered by the condition's own atomics.
    sequence_.fetch_add(1, std::memory_order_relaxed);
    futex(sequence_, FUTEX_WAKE, 1, nullptr);
  }
}

}  // namespace rtbus::detail
