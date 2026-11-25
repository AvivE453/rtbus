#include "rtbus/detail/segment.hpp"

#include <algorithm>
#include <new>
#include <stdexcept>

namespace rtbus::detail {
namespace {

constexpr std::uint64_t kMagic = 0x4745'5353'5542'5452;  // "RTBUSSEG" in little-endian bytes
constexpr std::uint32_t kVersion = 1;
constexpr std::size_t kMaxTopicLength = 200;

constexpr std::size_t round_up(std::size_t value, std::size_t alignment) {
  return (value + alignment - 1) / alignment * alignment;
}

constexpr std::size_t kSlotsOffset = round_up(sizeof(SegmentHeader), alignof(SubscriberSlot));
constexpr std::size_t kPoolOffset = kSlotsOffset + kMaxSubscribers * sizeof(SubscriberSlot);
static_assert(kPoolOffset % ChunkPool::kAlignment == 0);

bool is_topic_character(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
         c == '.' || c == '-';
}

std::byte* at(void* base, std::size_t offset) { return static_cast<std::byte*>(base) + offset; }

}  // namespace

std::string segment_name(const std::string& topic) {
  if (topic.empty() || topic.size() > kMaxTopicLength ||
      !std::all_of(topic.begin(), topic.end(), is_topic_character)) {
    throw std::invalid_argument("invalid topic name '" + topic +
                                "': use 1 to 200 characters of A-Z, a-z, 0-9, '_', '.' or '-'");
  }
  return "/rtbus." + topic;
}

std::size_t segment_size(std::size_t payload_size) {
  return kPoolOffset + ChunkPool::bytes_needed(kChunkCount, payload_size);
}

SegmentView::SegmentView(void* base, ChunkPool pool)
    : header_(reinterpret_cast<SegmentHeader*>(base)),
      slots_(reinterpret_cast<SubscriberSlot*>(at(base, kSlotsOffset))),
      pool_(pool) {}

SegmentView SegmentView::initialize(void* base, const MessageType& type) {
  auto* header = new (base) SegmentHeader{};
  header->magic = kMagic;
  header->version = kVersion;
  header->type_fingerprint = type.fingerprint;
  header->payload_size = type.size;
  for (std::uint32_t i = 0; i < kMaxSubscribers; ++i) {
    new (at(base, kSlotsOffset + i * sizeof(SubscriberSlot))) SubscriberSlot;
  }
  SegmentView view(base, ChunkPool::create(at(base, kPoolOffset), kChunkCount, type.size));
  // Release: a subscriber that sees kReady (acquire, in attach()) also sees every field and
  // slot written above.
  header->state.store(SegmentState::kReady, std::memory_order_release);
  return view;
}

std::optional<SegmentView> SegmentView::attach(void* base, std::size_t mapped_size,
                                               const MessageType& type) {
  if (mapped_size < sizeof(SegmentHeader)) {
    return std::nullopt;
  }
  const auto& header = *reinterpret_cast<const SegmentHeader*>(base);
  // Acquire, paired with the release in initialize().
  if (header.state.load(std::memory_order_acquire) != SegmentState::kReady) {
    return std::nullopt;
  }
  if (header.magic != kMagic || header.version != kVersion ||
      mapped_size < segment_size(header.payload_size)) {
    throw std::runtime_error("not an rtbus segment of this version");
  }
  if (header.type_fingerprint != type.fingerprint || header.payload_size != type.size) {
    throw std::invalid_argument("the topic carries a different message type");
  }
  return SegmentView(base, ChunkPool::attach(at(base, kPoolOffset), kChunkCount, type.size));
}

}  // namespace rtbus::detail
