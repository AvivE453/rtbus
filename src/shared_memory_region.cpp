#include "rtbus/shared_memory_region.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>
#include <utility>

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

// An owner's name is removed on failure, as in create(); anyone else's is left alone.
[[noreturn]] void fail_to_map(const std::string& name, bool owner, const std::string& call) {
  const std::string what = call + "(" + name + ")";
  if (owner) {
    unlink_and_throw(name, what);
  }
  throw_errno(what);
}

}  // namespace

SharedMemoryRegion SharedMemoryRegion::create(const std::string& name, std::size_t size) {
  FileDescriptor fd(::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600));
  if (!fd.valid()) {
    throw_errno("shm_open(" + name + ")");
  }
  if (::ftruncate(fd.get(), static_cast<off_t>(size)) != 0) {
    unlink_and_throw(name, "ftruncate(" + name + ")");
  }
  return map(name, std::move(fd), /*owner=*/true);
}

SharedMemoryRegion SharedMemoryRegion::open(const std::string& name) {
  FileDescriptor fd(::shm_open(name.c_str(), O_RDWR, 0));
  if (!fd.valid()) {
    throw_errno("shm_open(" + name + ")");
  }
  return map(name, std::move(fd), /*owner=*/false);
}

SharedMemoryRegion SharedMemoryRegion::map(std::string name, FileDescriptor fd, bool owner) {
  struct stat info {};
  if (::fstat(fd.get(), &info) != 0) {
    fail_to_map(name, owner, "fstat");
  }
  const auto size = static_cast<std::size_t>(info.st_size);
  void* data = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
  if (data == MAP_FAILED) {
    fail_to_map(name, owner, "mmap");
  }
  return {std::move(name), std::move(fd), data, size, owner};
}

SharedMemoryRegion::SharedMemoryRegion(std::string name, FileDescriptor fd, void* data,
                                       std::size_t size, bool owner) noexcept
    : name_(std::move(name)), fd_(std::move(fd)), data_(data), size_(size), owner_(owner) {}

SharedMemoryRegion::~SharedMemoryRegion() { release(); }

SharedMemoryRegion::SharedMemoryRegion(SharedMemoryRegion&& other) noexcept
    : name_(std::move(other.name_)),
      fd_(std::move(other.fd_)),
      data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      owner_(std::exchange(other.owner_, false)) {}

SharedMemoryRegion& SharedMemoryRegion::operator=(SharedMemoryRegion&& other) noexcept {
  if (this != &other) {
    release();
    name_ = std::move(other.name_);
    fd_ = std::move(other.fd_);
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
  // Unlink before closing the descriptor. Closing releases this descriptor's locks, and the
  // publisher's lock is what stops another process from taking the name over; unlinking
  // after that could remove the name of the other process's new segment.
  if (owner_) {
    ::shm_unlink(name_.c_str());
    owner_ = false;
  }
  fd_.reset();
}

}  // namespace rtbus
