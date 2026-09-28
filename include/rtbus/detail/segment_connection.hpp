#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "rtbus/detail/message_type.hpp"
#include "rtbus/detail/segment.hpp"
#include "rtbus/shared_memory_region.hpp"

namespace rtbus::detail {

// One subscriber's attachment to a topic's segment: its own mapping and descriptor, and one
// claimed subscriber slot whose lock it holds for as long as it lives.
class SegmentConnection {
 public:
  // Attaches to the segment and claims a slot with a queue of `capacity` chunks.
  // Returns nullptr when there is nothing to connect to yet: no segment, one still being set
  // up or already closed, one left behind by a dead publisher, or no slot free right now.
  // Throws std::invalid_argument if the segment carries another message type, and
  // std::runtime_error if every slot belongs to a live subscriber.
  static std::unique_ptr<SegmentConnection> connect(const std::string& segment_name,
                                                    const MessageType& type,
                                                    std::uint32_t capacity);

  // Marks the slot as leaving, so the publisher reclaims it, and drops the slot lock.
  ~SegmentConnection();

  SegmentConnection(const SegmentConnection&) = delete;
  SegmentConnection& operator=(const SegmentConnection&) = delete;
  SegmentConnection(SegmentConnection&&) = delete;
  SegmentConnection& operator=(SegmentConnection&&) = delete;

  // The next chunk, if any. The caller reads it through payload() and then must finish() it.
  [[nodiscard]] std::optional<std::uint32_t> take();
  [[nodiscard]] const void* payload(std::uint32_t chunk) { return segment_.pool().payload(chunk); }
  void finish(std::uint32_t chunk);

  // Sleeps until a chunk arrives, the publisher closes the segment, `stop` is set (followed
  // by wake()), or `timeout` passes. May also return early for no reason.
  void wait(const std::atomic<bool>& stop, std::chrono::nanoseconds timeout);
  void wake() { slot().wake.notify(); }

  // Cheap: true once the publisher closed the segment normally.
  [[nodiscard]] bool closed() const;
  // Also true if the publisher died, or a new publisher replaced the segment. Makes system
  // calls, so it is meant for occasional checks.
  [[nodiscard]] bool publisher_gone() const;

  // Messages this subscriber lost because its queue was full.
  [[nodiscard]] std::uint64_t dropped() const { return slot().ring.dropped(); }

 private:
  SegmentConnection(SharedMemoryRegion region, SegmentView segment, std::uint32_t slot_index);

  [[nodiscard]] SubscriberSlot& slot() const { return segment_.slot(slot_index_); }

  SharedMemoryRegion region_;
  SegmentView segment_;
  std::uint32_t slot_index_;
};

}  // namespace rtbus::detail
