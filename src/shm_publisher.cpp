#include "rtbus/detail/shm_publisher.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "rtbus/detail/byte_lock.hpp"
#include "rtbus/file_descriptor.hpp"

namespace rtbus::detail {
namespace {

[[noreturn]] void throw_errno(const std::string& what) {
  throw std::system_error(errno, std::generic_category(), what);
}

bool is_unlinked(int fd) {
  struct stat info {};
  if (::fstat(fd, &info) != 0) {
    throw_errno("fstat");
  }
  return info.st_nlink == 0;
}

// Returns the segment `name`, freshly created, sized and mapped, with the publisher lock held
// through its descriptor. Whoever holds that lock owns the name, and a name is only ever
// removed by its lock holder, so of several processes racing here exactly one wins.
SharedMemoryRegion take_over_segment(const std::string& name, std::size_t size) {
  while (true) {
    FileDescriptor fd(::shm_open(name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
    const bool created = fd.valid();
    if (!created) {
      if (errno != EEXIST) {
        throw_errno("shm_open(" + name + ")");
      }
      fd.reset(::shm_open(name.c_str(), O_RDWR, 0));
      if (!fd.valid()) {
        if (errno == ENOENT) {
          continue;  // removed since our first attempt: try to create it again
        }
        throw_errno("shm_open(" + name + ")");
      }
    }

    if (!try_lock_byte(fd.get(), kPublisherLockByte)) {
      if (created) {
        continue;  // another process took our new file for a leftover and is removing it
      }
      throw std::runtime_error("topic segment '" + name + "' already has a live publisher");
    }
    if (is_unlinked(fd.get())) {
      continue;  // removed between our open and our lock: the name now means another file
    }
    if (!created) {
      // We hold the lock of a segment nobody else holds: its publisher died. Remove it and
      // start over with a fresh one. Subscribers still attached notice it was unlinked.
      ::shm_unlink(name.c_str());
      continue;
    }
    if (::ftruncate(fd.get(), static_cast<off_t>(size)) != 0) {
      const int error = errno;
      ::shm_unlink(name.c_str());
      throw std::system_error(error, std::generic_category(), "ftruncate(" + name + ")");
    }
    return SharedMemoryRegion::map(name, std::move(fd), /*owner=*/true);
  }
}

}  // namespace

ShmPublisher::ShmPublisher(const std::string& topic, const MessageType& type)
    : region_(take_over_segment(segment_name(topic), segment_size(type.size))),
      segment_(SegmentView::initialize(region_.data(), type)) {}

ShmPublisher::~ShmPublisher() {
  // Release: a subscriber woken below that sees kClosed has also seen every message
  // published before it.
  segment_.header().state.store(SegmentState::kClosed, std::memory_order_release);
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    segment_.slot(i).wake.notify();
  }
}

std::optional<std::uint32_t> ShmPublisher::loan() {
  if (loans_outstanding_ == kMaxLoans) {
    return std::nullopt;
  }
  const std::optional<std::uint32_t> chunk = segment_.pool().allocate();
  if (chunk) {
    ++loans_outstanding_;
  }
  return chunk;
}

void ShmPublisher::discard(std::uint32_t chunk) {
  segment_.pool().release(chunk);
  --loans_outstanding_;
}

void ShmPublisher::publish(std::uint32_t chunk) {
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    SubscriberSlot& slot = segment_.slot(i);
    // Acquire, paired with the release that activates a slot (SegmentConnection) or marks it
    // leaving: we see the reset ring, or every pop the subscriber did before leaving.
    switch (slot.state.load(std::memory_order_acquire)) {
      case SlotState::kActive:
        deliver(slot, chunk);
        break;
      case SlotState::kLeaving:
        reclaim(slot);
        break;
      case SlotState::kFree:
        break;
    }
  }
  segment_.pool().release(chunk);
  --loans_outstanding_;
}

void ShmPublisher::deliver(SubscriberSlot& slot, std::uint32_t chunk) {
  ChunkPool& pool = segment_.pool();
  // The queue's reference is taken before the push: once the index is in the queue, the
  // subscriber may pop and release it at any moment.
  pool.retain(chunk);
  if (const std::optional<std::uint32_t> evicted = slot.ring.push(chunk)) {
    pool.release(*evicted);
  }
  slot.wake.notify();
}

// The slot's subscriber has stopped using it, by leaving or by dying: release every chunk
// the slot still references, then make it free for the next subscriber.
void ShmPublisher::reclaim(SubscriberSlot& slot) {
  ChunkPool& pool = segment_.pool();
  while (const std::optional<std::uint32_t> chunk = slot.ring.pop()) {
    pool.release(*chunk);
  }
  const std::uint32_t in_hand = slot.in_hand.exchange(kNoChunk, std::memory_order_relaxed);
  if (in_hand != kNoChunk) {
    pool.release(in_hand);
  }
  // Release, paired with the acquire in SegmentConnection's claim: the next subscriber resets
  // the ring only after our pops above are done.
  slot.state.store(SlotState::kFree, std::memory_order_release);
}

std::size_t ShmPublisher::subscriber_count() const {
  std::size_t count = 0;
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    // Relaxed: a snapshot for monitoring and tests; nothing is read through it.
    if (segment_.slot(i).state.load(std::memory_order_relaxed) == SlotState::kActive) {
      ++count;
    }
  }
  return count;
}

}  // namespace rtbus::detail
