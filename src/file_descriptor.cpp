#include "rtbus/file_descriptor.hpp"

#include <unistd.h>

#include <utility>

namespace rtbus {

FileDescriptor::~FileDescriptor() { reset(); }

FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)) {}

FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) noexcept {
  reset(std::exchange(other.fd_, -1));
  return *this;
}

void FileDescriptor::reset(int fd) noexcept {
  // On Linux, close() releases the descriptor even when it reports an error,
  // so retrying (e.g. on EINTR) could close a descriptor another thread just opened.
  if (fd_ >= 0) {
    ::close(fd_);
  }
  fd_ = fd;
}

}  // namespace rtbus
