#pragma once

#include <cstddef>
#include <string>

#include "rtbus/file_descriptor.hpp"

namespace rtbus {

// A named POSIX shared-memory object (/dev/shm/<name>) mapped into this process.
// The owner (the process that create()s the region) unlinks the name on destruction;
// processes that open() an existing region only unmap it.
// The region keeps its file descriptor open, because file locks on the object belong to
// the descriptor (see detail/byte_lock.hpp).
// Move-only. Names must start with '/'.
class SharedMemoryRegion {
 public:
  // Fails if `name` already exists, so two creators can never share a region by accident.
  // [[nodiscard]]: a discarded result would unlink the region it just created.
  [[nodiscard]] static SharedMemoryRegion create(const std::string& name, std::size_t size);
  [[nodiscard]] static SharedMemoryRegion open(const std::string& name);
  // Maps the whole object that `fd` refers to, for callers that open or size the object
  // themselves. The object must not be empty: an empty mapping is an error.
  [[nodiscard]] static SharedMemoryRegion map(std::string name, FileDescriptor fd, bool owner);

  ~SharedMemoryRegion();

  SharedMemoryRegion(const SharedMemoryRegion&) = delete;
  SharedMemoryRegion& operator=(const SharedMemoryRegion&) = delete;
  SharedMemoryRegion(SharedMemoryRegion&& other) noexcept;
  SharedMemoryRegion& operator=(SharedMemoryRegion&& other) noexcept;

  [[nodiscard]] void* data() const noexcept { return data_; }
  [[nodiscard]] std::size_t size() const noexcept { return size_; }
  [[nodiscard]] const std::string& name() const noexcept { return name_; }
  [[nodiscard]] int fd() const noexcept { return fd_.get(); }

 private:
  SharedMemoryRegion(std::string name, FileDescriptor fd, void* data, std::size_t size,
                     bool owner) noexcept;
  void release() noexcept;

  std::string name_;
  FileDescriptor fd_;
  void* data_ = nullptr;
  std::size_t size_ = 0;
  bool owner_ = false;
};

}  // namespace rtbus
