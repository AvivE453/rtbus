#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>

#include "rtbus/detail/chunk_pool.hpp"
#include "rtbus/detail/index_ring.hpp"
#include "rtbus/detail/message_type.hpp"
#include "rtbus/detail/wake_signal.hpp"

namespace rtbus::detail {

// The shared-memory segment behind one topic, /dev/shm/rtbus.<topic>. The publisher creates
// and owns it; subscribers attach to it. It holds offsets and indices, never pointers:
//
//   [ SegmentHeader | SubscriberSlot x kMaxSubscribers | ChunkPool ]

enum class SegmentState : std::uint8_t { kInitializing = 0, kReady = 1, kClosed = 2 };

struct SegmentHeader {
  std::uint64_t magic;
  std::uint32_t version;
  std::uint64_t type_fingerprint;
  std::uint64_t payload_size;
  // A new object is all zeros, so it starts as kInitializing. The publisher stores kReady
  // last; a subscriber reads nothing else until it has seen kReady.
  std::atomic<SegmentState> state;
};

enum class SlotState : std::uint8_t { kFree = 0, kActive = 1, kLeaving = 2 };

inline constexpr std::uint32_t kNoChunk = std::numeric_limits<std::uint32_t>::max();

// One subscriber's part of the segment.
//
// The slot's state changes hands like this:
//   kFree    -> kActive   by the subscriber that holds the slot's lock, after resetting `ring`
//   kActive  -> kLeaving  by that subscriber when it leaves, or by a later subscriber that
//                         finds it dead
//   kLeaving -> kFree     by the publisher, after releasing every chunk the slot still holds
// The publisher pushes only into kActive slots, and only the publisher frees a slot, so a
// ring is never reset while the publisher may still be pushing into it.
struct alignas(64) SubscriberSlot {
  std::atomic<SlotState> state{SlotState::kFree};
  // The chunk whose callback is running, so the publisher can release it if the subscriber
  // dies in the middle.
  std::atomic<std::uint32_t> in_hand{kNoChunk};
  WakeSignal wake;
  IndexRing ring{IndexRing::kMaxCapacity};
};

static_assert(std::atomic<SegmentState>::is_always_lock_free);
static_assert(std::atomic<SlotState>::is_always_lock_free);
static_assert(std::is_trivially_destructible_v<SubscriberSlot>);

inline constexpr std::uint32_t kMaxSubscribers = 8;
// Loans the publisher may hold at once (a user's loan, plus the one publish(const T&) takes).
inline constexpr std::uint32_t kMaxLoans = 2;
// Chosen so that allocation cannot fail while loans stay within kMaxLoans: every subscriber
// can hold a full queue plus the chunk its callback is reading.
inline constexpr std::uint32_t kChunkCount =
    kMaxSubscribers * (IndexRing::kMaxCapacity + 1) + kMaxLoans;

// "/rtbus.<topic>". Throws std::invalid_argument unless the topic is 1 to 200 characters of
// [A-Za-z0-9_.-]: the name becomes a file name in /dev/shm.
std::string segment_name(const std::string& topic);

// Bytes of shared memory a segment for messages of `payload_size` bytes needs.
std::size_t segment_size(std::size_t payload_size);

// A process-local view of a segment mapped at some address in this process.
class SegmentView {
 public:
  // Publisher: lays out a new, all-zero segment of segment_size(type.size) bytes, then marks
  // it ready.
  static SegmentView initialize(void* base, const MessageType& type);

  // Subscriber: returns std::nullopt while the segment is not ready (still being set up, or
  // closed). Throws std::runtime_error if the memory is not an rtbus segment of this
  // version, and std::invalid_argument if it carries a different message type.
  static std::optional<SegmentView> attach(void* base, std::size_t mapped_size,
                                           const MessageType& type);

  [[nodiscard]] SegmentHeader& header() const { return *header_; }
  [[nodiscard]] SubscriberSlot& slot(std::uint32_t index) const { return slots_[index]; }
  [[nodiscard]] ChunkPool& pool() { return pool_; }

 private:
  SegmentView(void* base, ChunkPool pool);

  SegmentHeader* header_;
  SubscriberSlot* slots_;
  ChunkPool pool_;
};

}  // namespace rtbus::detail
