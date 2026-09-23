#pragma once

#include <cstddef>
#include <string>

namespace rtbus {

// A named POSIX shared-memory object (/dev/shm/<name>) mapped into this process.
// The process that create()s the region owns the name and unlinks it on
// destruction; processes that open() an existing region only unmap it.
// Move-only. Names must start with '/'.
class SharedMemoryRegion {
 public:
  // Fails if `name` already exists, so two creators can never share a region by accident.
  static SharedMemoryRegion create(const std::string& name, std::size_t size);
  static SharedMemoryRegion open(const std::string& name);

  ~SharedMemoryRegion();

  SharedMemoryRegion(const SharedMemoryRegion&) = delete;
  SharedMemoryRegion& operator=(const SharedMemoryRegion&) = delete;
  SharedMemoryRegion(SharedMemoryRegion&& other) noexcept;
  SharedMemoryRegion& operator=(SharedMemoryRegion&& other) noexcept;

  void* data() const noexcept { return data_; }
  std::size_t size() const noexcept { return size_; }
  const std::string& name() const noexcept { return name_; }

 private:
  SharedMemoryRegion(std::string name, void* data, std::size_t size, bool owner) noexcept;
  void release() noexcept;

  std::string name_;
  void* data_ = nullptr;
  std::size_t size_ = 0;
  bool owner_ = false;
};

}  // namespace rtbus
