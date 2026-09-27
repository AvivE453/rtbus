#pragma once

#include <atomic>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>

namespace rtbus::detail {

// A lock-free atomic works across processes because it is a plain machine instruction on a
// memory address. One that is not lock-free hides a mutex inside the process that uses it,
// which the other process can neither see nor respect.
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

// A fixed-capacity FIFO of chunk indices, built to live in shared memory and be used by one
// producer (the publisher) and one consumer (a subscriber), possibly in different processes.
// The producer never waits: pushing into a full ring evicts the oldest index and returns it,
// so the caller can release the chunk that index refers to.
//
// Because each process maps shared memory at a different address, the ring stores no
// pointers: only indices and counters.
//
// Positions (head_, tail_) count up forever and are reduced modulo the capacity only to pick
// a slot. At 64 bits they cannot wrap in practice, so `tail_ - head_` is always the number
// of queued indices.
class IndexRing {
 public:
  static constexpr std::uint32_t kMaxCapacity = 16;

  explicit IndexRing(std::uint32_t capacity) : capacity_(capacity) {
    if (capacity == 0 || capacity > kMaxCapacity) {
      throw std::invalid_argument("IndexRing capacity must be between 1 and 16");
    }
  }

  // Producer only. Returns the evicted index when the ring was full; the caller now owns it.
  [[nodiscard]] std::optional<std::uint32_t> push(std::uint32_t index) {
    // Relaxed: only the producer writes tail_, so it always reads its own latest value.
    const std::uint64_t tail = tail_.load(std::memory_order_relaxed);
    // Acquire, paired with the release in pop(): if the consumer has moved head_ past a
    // slot, its read of that slot happened before we overwrite it below.
    std::uint64_t head = head_.load(std::memory_order_acquire);

    std::optional<std::uint32_t> evicted;
    if (tail - head == capacity_) {
      const std::uint32_t oldest = slot(head).load(std::memory_order_relaxed);
      // The consumer may be popping this same index right now. Whoever moves head_ from
      // `head` to `head + 1` owns it. This must be the strong CAS: a weak one may fail
      // spuriously while head_ still equals `head`, and we would then overwrite an index
      // that nobody took.
      // Acquire covers the failure case: the consumer won, and its read of the slot must
      // happen before we overwrite it.
      if (head_.compare_exchange_strong(head, head + 1, std::memory_order_acquire)) {
        evicted = oldest;
        // Relaxed: a statistic that orders nothing else.
        dropped_.fetch_add(1, std::memory_order_relaxed);
      }
    }

    slot(tail).store(index, std::memory_order_relaxed);
    // Release, paired with the acquire in pop(): a consumer that sees the new tail_ also
    // sees the index stored in the slot above.
    tail_.store(tail + 1, std::memory_order_release);
    return evicted;
  }

  // Consumer only. Returns std::nullopt when the ring is empty; never waits.
  [[nodiscard]] std::optional<std::uint32_t> pop() {
    // Relaxed: a stale value only makes the CAS below fail and reload it.
    std::uint64_t head = head_.load(std::memory_order_relaxed);
    // Acquire, paired with the release in push(): makes the slot at `head` visible.
    while (head != tail_.load(std::memory_order_acquire)) {
      const std::uint32_t index = slot(head).load(std::memory_order_relaxed);
      // Success means the producer had not evicted `head`, so `index` is the value pushed at
      // that position. Release, paired with the acquire in push(): our read of the slot
      // finishes before the producer sees it as free and overwrites it.
      // On failure the producer evicted it and `head` now holds the new head: retry.
      if (head_.compare_exchange_weak(head, head + 1, std::memory_order_release,
                                      std::memory_order_relaxed)) {
        return index;
      }
    }
    return std::nullopt;
  }

  // Consumer only.
  [[nodiscard]] bool empty() const {
    // Acquire, as in pop(). head_ needs no ordering: only our own pops and the producer's
    // evictions move it, and a stale value is possible only while the ring is full, where it
    // still reads as non-empty.
    return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_acquire);
  }

  // Indices evicted by push() because the consumer fell behind.
  [[nodiscard]] std::uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }

 private:
  std::atomic<std::uint32_t>& slot(std::uint64_t position) { return slots_[position % capacity_]; }

  // head_ is written by the consumer, tail_ by the producer. On separate cache lines, a write
  // to one does not force the other side to reload the line holding the other (false sharing).
  alignas(64) std::atomic<std::uint64_t> head_{0};
  // Set once in the constructor, before any other process can reach the ring.
  const std::uint32_t capacity_;
  alignas(64) std::atomic<std::uint64_t> tail_{0};
  std::atomic<std::uint64_t> dropped_{0};
  alignas(64) std::atomic<std::uint32_t> slots_[kMaxCapacity]{};
};

// Shared memory is unmapped, never destroyed, so no destructor may have work to do.
static_assert(std::is_trivially_destructible_v<IndexRing>);

}  // namespace rtbus::detail
