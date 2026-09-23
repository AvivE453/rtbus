#include "rtbus/shared_memory_region.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>
#include <utility>

#include "rtbus/file_descriptor.hpp"

namespace rtbus {
namespace {

[[noreturn]] void throw_errno(const std::string& what) {
  throw std::system_error(errno, std::generic_category(), what);
}

// Used once the name exists in /dev/shm, so a failed create() leaves nothing behind.
[[noreturn]] void unlink_and_throw(const std::string& name, const std::string& what) {
  const int error = errno;
  ::shm_unlink(name.c_str());
  throw std::system_error(error, std::generic_category(), what);
}

void* map_shared(int fd, std::size_t size) {
  return ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

}  // namespace

// In both factories the descriptor is closed on return: the mapping alone keeps
// the memory alive, so there is no reason to hold an fd for the region's lifetime.

SharedMemoryRegion SharedMemoryRegion::create(const std::string& name, std::size_t size) {
  FileDescriptor fd(::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600));
  if (!fd.valid()) {
    throw_errno("shm_open(" + name + ")");
  }
  if (::ftruncate(fd.get(), static_cast<off_t>(size)) != 0) {
    unlink_and_throw(name, "ftruncate(" + name + ")");
  }
  void* data = map_shared(fd.get(), size);
  if (data == MAP_FAILED) {
    unlink_and_throw(name, "mmap(" + name + ")");
  }
  return {name, data, size, /*owner=*/true};
}

SharedMemoryRegion SharedMemoryRegion::open(const std::string& name) {
  FileDescriptor fd(::shm_open(name.c_str(), O_RDWR, 0));
  if (!fd.valid()) {
    throw_errno("shm_open(" + name + ")");
  }
  struct stat info {};
  if (::fstat(fd.get(), &info) != 0) {
    throw_errno("fstat(" + name + ")");
  }
  const auto size = static_cast<std::size_t>(info.st_size);
  void* data = map_shared(fd.get(), size);
  if (data == MAP_FAILED) {
    throw_errno("mmap(" + name + ")");
  }
  return {name, data, size, /*owner=*/false};
}

SharedMemoryRegion::SharedMemoryRegion(std::string name, void* data, std::size_t size,
                                       bool owner) noexcept
    : name_(std::move(name)), data_(data), size_(size), owner_(owner) {}

SharedMemoryRegion::~SharedMemoryRegion() { release(); }

SharedMemoryRegion::SharedMemoryRegion(SharedMemoryRegion&& other) noexcept
    : name_(std::move(other.name_)),
      data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      owner_(std::exchange(other.owner_, false)) {}

SharedMemoryRegion& SharedMemoryRegion::operator=(SharedMemoryRegion&& other) noexcept {
  if (this != &other) {
    release();
    name_ = std::move(other.name_);
    data_ = std::exchange(other.data_, nullptr);
    size_ = std::exchange(other.size_, 0);
    owner_ = std::exchange(other.owner_, false);
  }
  return *this;
}

void SharedMemoryRegion::release() noexcept {
  if (data_ != nullptr) {
    ::munmap(data_, size_);
    data_ = nullptr;
  }
  if (owner_) {
    ::shm_unlink(name_.c_str());
    owner_ = false;
  }
}

}  // namespace rtbus
