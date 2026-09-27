#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "rtbus/detail/message_type.hpp"
#include "rtbus/detail/segment.hpp"
#include "rtbus/shared_memory_region.hpp"

namespace rtbus::detail {

// The publishing side of one topic: owns the topic's segment and hands chunks to subscribers.
// Used by one thread at a time, since the segment's structures have a single producer.
class ShmPublisher {
 public:
  // Creates the topic's segment and holds its publisher lock. A segment left behind by a
  // publisher that died is replaced. Throws std::runtime_error if a live publisher, in this
  // process or another, already owns the topic.
  ShmPublisher(const std::string& topic, const MessageType& type);

  // Marks the segment closed and wakes every subscriber so it disconnects. The name is
  // removed; subscribers keep their mappings until they let go.
  ~ShmPublisher();

  ShmPublisher(const ShmPublisher&) = delete;
  ShmPublisher& operator=(const ShmPublisher&) = delete;
  ShmPublisher(ShmPublisher&&) = delete;
  ShmPublisher& operator=(ShmPublisher&&) = delete;

  // A free chunk for the caller to fill, or std::nullopt if none is free, which happens only
  // when more than kMaxLoans loans are outstanding.
  [[nodiscard]] std::optional<std::uint32_t> loan() { return segment_.pool().allocate(); }
  [[nodiscard]] void* payload(std::uint32_t chunk) { return segment_.pool().payload(chunk); }
  // Hands a loaned chunk to every connected subscriber and gives up the loan's reference.
  void publish(std::uint32_t chunk);
  // Returns an unpublished loan to the pool.
  void discard(std::uint32_t chunk) { segment_.pool().release(chunk); }

  [[nodiscard]] std::size_t subscriber_count() const;

 private:
  void deliver(SubscriberSlot& slot, std::uint32_t chunk);
  void reclaim(SubscriberSlot& slot);

  SharedMemoryRegion region_;
  SegmentView segment_;
};

}  // namespace rtbus::detail
