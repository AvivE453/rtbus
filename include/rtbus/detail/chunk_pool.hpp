#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>

namespace rtbus::detail {

// A fixed number of equal-size chunks in shared memory, each holding one message, with a
// reference count per chunk: how many holders (subscriber queues, a subscriber running its
// callback, the publisher filling it) still need it. At zero the chunk is free.
//
// Only the publisher allocates, so a chunk goes from free to in-use in exactly one place and
// needs no compare-and-swap. Any process may release.
//
// A ChunkPool object is a process-local view: it stores this process's address of the
// memory, never anything inside it. Copying the view is cheap and shares the same chunks.
class ChunkPool {
 public:
  static constexpr std::size_t kAlignment = 64;

  // `memory` must be aligned to kAlignment (mmap returns page-aligned memory).
  [[nodiscard]] static std::size_t bytes_needed(std::uint32_t chunk_count,
                                                std::size_t payload_size) {
    return chunk_count * stride_for(payload_size);
  }

  // Called once by the process that owns the memory, before any other process uses it.
  [[nodiscard]] static ChunkPool create(void* memory, std::uint32_t chunk_count,
                                        std::size_t payload_size) {
    ChunkPool pool(memory, chunk_count, payload_size);
    for (std::uint32_t index = 0; index < chunk_count; ++index) {
      new (pool.chunk(index)) ChunkHeader;
    }
    return pool;
  }

  // A view of a pool that create() already set up, from any process and at any address.
  [[nodiscard]] static ChunkPool attach(void* memory, std::uint32_t chunk_count,
                                        std::size_t payload_size) {
    return {memory, chunk_count, payload_size};
  }

  // Publisher only. Returns the lowest-numbered free chunk, whose single reference now
  // belongs to the caller, or std::nullopt if every chunk is in use. Scanning from zero every
  // time keeps reuse on the same few chunks while subscribers keep up: they stay in cache, and
  // the pages of the rest of the pool are never touched, so the OS never has to back them.
  [[nodiscard]] std::optional<std::uint32_t> allocate() {
    for (std::uint32_t index = 0; index < chunk_count_; ++index) {
      std::atomic<std::uint32_t>& references = header(index).references;
      // Acquire, paired with the release in release(): every former holder finished with
      // the payload before we hand the chunk out to be overwritten.
      if (references.load(std::memory_order_acquire) == 0) {
        // Relaxed: at zero no other process can reach this chunk, so nothing races with
        // this store. Others learn the index only through IndexRing, whose release/acquire
        // pair orders it.
        references.store(1, std::memory_order_relaxed);
        return index;
      }
    }
    return std::nullopt;
  }

  // Adds a holder. The caller must itself hold a reference, so the count is not zero here.
  void retain(std::uint32_t index) {
    // Relaxed, as when copying a std::shared_ptr: the caller's own reference keeps the chunk
    // alive, and the new holder receives the index through IndexRing, which orders it.
    header(index).references.fetch_add(1, std::memory_order_relaxed);
  }

  // Drops a holder. The chunk is free once the count reaches zero.
  void release(std::uint32_t index) {
    // Release: this holder's reads of the payload happen before the chunk can be seen as
    // free. Every later decrement continues the same "release sequence", so the acquire in
    // allocate() that reads zero synchronizes with all holders, not only the last one.
    header(index).references.fetch_sub(1, std::memory_order_release);
  }

  [[nodiscard]] void* payload(std::uint32_t index) const {
    return chunk(index) + sizeof(ChunkHeader);
  }

 private:
  // A whole cache line, so the count never shares a line with a neighbour's payload.
  struct alignas(kAlignment) ChunkHeader {
    std::atomic<std::uint32_t> references{0};
  };

  static std::size_t stride_for(std::size_t payload_size) {
    const std::size_t aligned_payload = (payload_size + kAlignment - 1) / kAlignment * kAlignment;
    return sizeof(ChunkHeader) + aligned_payload;
  }

  ChunkPool(void* memory, std::uint32_t chunk_count, std::size_t payload_size)
      : base_(static_cast<std::byte*>(memory)),
        chunk_count_(chunk_count),
        stride_(stride_for(payload_size)) {}

  [[nodiscard]] std::byte* chunk(std::uint32_t index) const { return base_ + index * stride_; }
  [[nodiscard]] ChunkHeader& header(std::uint32_t index) const {
    return *reinterpret_cast<ChunkHeader*>(chunk(index));
  }

  std::byte* base_;
  std::uint32_t chunk_count_;
  std::size_t stride_;
};

}  // namespace rtbus::detail
