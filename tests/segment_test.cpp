#include "rtbus/detail/segment.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

#include "rtbus/shared_memory_region.hpp"
#include "unique_shm_name.hpp"

namespace rtbus::detail {
namespace {

struct Pose {
  double x;
  double y;
};

SharedMemoryRegion zeroed_segment_memory(std::size_t payload_size) {
  return SharedMemoryRegion::create(test_support::unique_shm_name(), segment_size(payload_size));
}

TEST(SegmentTest, SegmentNameIsPrefixedTopic) {
  EXPECT_EQ(segment_name("imu"), "/rtbus.imu");
  EXPECT_EQ(segment_name("robot_1.pose-raw"), "/rtbus.robot_1.pose-raw");
}

TEST(SegmentTest, TopicNamesThatCannotBeFileNamesAreRejected) {
  EXPECT_THROW(static_cast<void>(segment_name("")), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(segment_name("a/b")), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(segment_name("with space")), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(segment_name(std::string(201, 'a'))), std::invalid_argument);
}

TEST(SegmentTest, SubscriberAttachesToInitializedSegmentOfSameType) {
  auto memory = zeroed_segment_memory(sizeof(Pose));
  SegmentView::initialize(memory.data(), message_type<Pose>());

  EXPECT_TRUE(SegmentView::attach(memory.data(), memory.size(), message_type<Pose>()).has_value());
}

TEST(SegmentTest, AttachWaitsUntilPublisherMarksSegmentReady) {
  auto memory = zeroed_segment_memory(sizeof(Pose));

  EXPECT_EQ(SegmentView::attach(memory.data(), memory.size(), message_type<Pose>()), std::nullopt);
}

TEST(SegmentTest, AttachWithDifferentMessageTypeIsRejected) {
  auto memory = zeroed_segment_memory(sizeof(Pose));
  SegmentView::initialize(memory.data(), message_type<Pose>());

  EXPECT_THROW(
      static_cast<void>(SegmentView::attach(memory.data(), memory.size(), message_type<int>())),
      std::invalid_argument);
}

TEST(SegmentTest, AttachToForeignMemoryIsRejected) {
  auto memory = zeroed_segment_memory(sizeof(Pose));
  SegmentView::initialize(memory.data(), message_type<Pose>());
  static_cast<SegmentHeader*>(memory.data())->magic = 0;

  EXPECT_THROW(
      static_cast<void>(SegmentView::attach(memory.data(), memory.size(), message_type<Pose>())),
      std::runtime_error);
}

TEST(SegmentTest, NewSegmentHasFreeSlotsAndAllChunksAvailable) {
  auto memory = zeroed_segment_memory(sizeof(Pose));
  SegmentView view = SegmentView::initialize(memory.data(), message_type<Pose>());

  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    EXPECT_EQ(view.slot(i).state.load(), SlotState::kFree);
    EXPECT_EQ(view.slot(i).in_hand.load(), kNoChunk);
  }
  for (std::uint32_t i = 0; i < kChunkCount; ++i) {
    ASSERT_EQ(view.pool().allocate(), i);
  }
  EXPECT_EQ(view.pool().allocate(), std::nullopt);
}

// Every part of the layout writes only inside its own bytes: filling every payload must not
// disturb the header or any slot.
TEST(SegmentTest, PayloadsDoNotOverlapHeaderOrSlots) {
  auto memory = zeroed_segment_memory(sizeof(Pose));
  SegmentView view = SegmentView::initialize(memory.data(), message_type<Pose>());

  for (std::uint32_t i = 0; i < kChunkCount; ++i) {
    *static_cast<Pose*>(view.pool().payload(i)) = Pose{-1.0, -1.0};
  }

  EXPECT_TRUE(SegmentView::attach(memory.data(), memory.size(), message_type<Pose>()).has_value());
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    EXPECT_EQ(view.slot(i).state.load(), SlotState::kFree);
    EXPECT_EQ(view.slot(i).ring.pop(), std::nullopt);
  }
}

}  // namespace
}  // namespace rtbus::detail
