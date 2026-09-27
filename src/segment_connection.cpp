#include "rtbus/detail/segment_connection.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cerrno>
#include <new>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "rtbus/detail/byte_lock.hpp"
#include "rtbus/file_descriptor.hpp"

namespace rtbus::detail {
namespace {

[[noreturn]] void throw_errno(const std::string& what) {
  throw std::system_error(errno, std::generic_category(), what);
}

struct stat file_status(int fd) {
  struct stat info {};
  if (::fstat(fd, &info) != 0) {
    throw_errno("fstat");
  }
  return info;
}

// The mapped segment of a live publisher, or std::nullopt if there is none to use yet.
std::optional<SharedMemoryRegion> open_live_segment(const std::string& name) {
  FileDescriptor fd(::shm_open(name.c_str(), O_RDWR, 0));
  if (!fd.valid()) {
    if (errno == ENOENT) {
      return std::nullopt;
    }
    throw_errno("shm_open(" + name + ")");
  }
  // A publisher that has created the file but not sized it yet.
  if (file_status(fd.get()).st_size == 0) {
    return std::nullopt;
  }
  // A leftover from a publisher that died: nobody holds its lock.
  if (!is_byte_locked(fd.get(), kPublisherLockByte)) {
    return std::nullopt;
  }
  return SharedMemoryRegion::map(name, std::move(fd), /*owner=*/false);
}

}  // namespace

std::unique_ptr<SegmentConnection> SegmentConnection::connect(const std::string& segment_name,
                                                              const MessageType& type,
                                                              std::uint32_t capacity) {
  std::optional<SharedMemoryRegion> region = open_live_segment(segment_name);
  if (!region) {
    return nullptr;
  }
  const std::optional<SegmentView> segment =
      SegmentView::attach(region->data(), region->size(), type);
  if (!segment) {
    return nullptr;
  }

  // Holding a slot's lock is what owns the slot. A lock we cannot take belongs to a live
  // subscriber; a lock we can take on a slot that is not free belongs to one that is gone.
  bool all_slots_live = true;
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    if (!try_lock_byte(region->fd(), slot_lock_byte(i))) {
      continue;
    }
    all_slots_live = false;
    SubscriberSlot& slot = segment->slot(i);
    // Acquire, paired with the publisher's release when it frees the slot: its last pops
    // happen before we reset the ring.
    switch (slot.state.load(std::memory_order_acquire)) {
      case SlotState::kFree:
        new (&slot.ring) IndexRing(capacity);
        slot.in_hand.store(kNoChunk, std::memory_order_relaxed);
        // Release, paired with the acquire in ShmPublisher::publish(): the publisher pushes
        // only into a ring it sees fully reset.
        slot.state.store(SlotState::kActive, std::memory_order_release);
        return std::unique_ptr<SegmentConnection>(
            new SegmentConnection(std::move(*region), *segment, i));
      case SlotState::kActive:
        // A live owner would still hold the lock, and one that left would have marked the
        // slot kLeaving: this subscriber died. Hand the slot to the publisher to reclaim.
        slot.state.store(SlotState::kLeaving, std::memory_order_release);
        break;
      case SlotState::kLeaving:
        break;  // the publisher has not reclaimed it yet
    }
    unlock_byte(region->fd(), slot_lock_byte(i));
  }
  if (all_slots_live) {
    throw std::runtime_error("topic segment '" + segment_name + "' already has " +
                             std::to_string(kMaxSubscribers) + " subscribers");
  }
  return nullptr;
}

SegmentConnection::SegmentConnection(SharedMemoryRegion region, SegmentView segment,
                                     std::uint32_t slot_index)
    : region_(std::move(region)), segment_(segment), slot_index_(slot_index) {}

SegmentConnection::~SegmentConnection() {
  // Release: every pop and release we did happens before the publisher reclaims the slot.
  // Harmless if the publisher is gone. Destroying region_ afterwards drops the slot lock.
  slot().state.store(SlotState::kLeaving, std::memory_order_release);
}

// If the process dies between the pop and the in_hand store here, or between clearing
// in_hand and the release in finish(), that one chunk is never released. Leaking a chunk in
// those few instructions is the accepted cost: any order that closes the gap can instead
// release a chunk twice, and a chunk released twice can be overwritten while still in use.
std::optional<std::uint32_t> SegmentConnection::take() {
  const std::optional<std::uint32_t> chunk = slot().ring.pop();
  if (chunk) {
    // Relaxed: the publisher reads in_hand only after seeing the slot marked kLeaving, and
    // that store (release) orders this one.
    slot().in_hand.store(*chunk, std::memory_order_relaxed);
  }
  return chunk;
}

void SegmentConnection::finish(std::uint32_t chunk) {
  slot().in_hand.store(kNoChunk, std::memory_order_relaxed);
  segment_.pool().release(chunk);
}

void SegmentConnection::wait(const std::atomic<bool>& stop, std::chrono::nanoseconds timeout) {
  // The WakeSignal protocol: take a ticket, re-check every reason to stay awake, then sleep.
  WakeSignal& signal = slot().wake;
  const std::uint32_t ticket = signal.prepare_wait();
  if (!slot().ring.empty() || stop.load() || closed()) {
    signal.cancel_wait();
    return;
  }
  signal.wait(ticket, timeout);
}

bool SegmentConnection::closed() const {
  // Acquire, paired with the release in ~ShmPublisher.
  return segment_.header().state.load(std::memory_order_acquire) == SegmentState::kClosed;
}

bool SegmentConnection::publisher_gone() const {
  if (closed()) {
    return true;
  }
  // Unlinked: a new publisher removed it as a leftover. Unlocked: the publisher died.
  return file_status(region_.fd()).st_nlink == 0 ||
         !is_byte_locked(region_.fd(), kPublisherLockByte);
}

}  // namespace rtbus::detail
