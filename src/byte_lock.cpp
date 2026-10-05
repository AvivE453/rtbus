#include "rtbus/detail/byte_lock.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>

namespace rtbus::detail {
namespace {

flock byte_range(short type, std::uint32_t byte) {
  flock range{};
  range.l_type = type;
  range.l_whence = SEEK_SET;
  range.l_start = static_cast<off_t>(byte);
  range.l_len = 1;
  range.l_pid = 0;  // required to be 0 for OFD locks
  return range;
}

[[noreturn]] void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

}  // namespace

bool try_lock_byte(int fd, std::uint32_t byte) {
  flock range = byte_range(F_WRLCK, byte);
  if (::fcntl(fd, F_OFD_SETLK, &range) == 0) {
    return true;
  }
  if (errno == EAGAIN || errno == EACCES) {
    return false;
  }
  throw_errno("fcntl(F_OFD_SETLK)");
}

void unlock_byte(int fd, std::uint32_t byte) {
  flock range = byte_range(F_UNLCK, byte);
  if (::fcntl(fd, F_OFD_SETLK, &range) != 0) {
    throw_errno("fcntl(F_OFD_SETLK, F_UNLCK)");
  }
}

bool is_byte_locked(int fd, std::uint32_t byte) {
  // F_OFD_GETLK asks "could this lock be taken?" and, if not, replaces l_type with the type of
  // a conflicting lock; if it could, l_type comes back as F_UNLCK.
  flock range = byte_range(F_WRLCK, byte);
  if (::fcntl(fd, F_OFD_GETLK, &range) != 0) {
    throw_errno("fcntl(F_OFD_GETLK)");
  }
  return range.l_type != F_UNLCK;
}

}  // namespace rtbus::detail
