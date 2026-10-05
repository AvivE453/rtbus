#pragma once

#include <cstdint>

namespace rtbus::detail {

// Exclusive, non-blocking locks on single bytes of a file, used as "I own this and I am
// alive" markers. The kernel drops a lock when its descriptor is closed, which includes the
// process dying in any way (kill -9 too), so a held lock proves its owner is alive.
//
// These are Linux open file description (OFD) locks, not classic POSIX (F_SETLK) locks. A
// POSIX lock belongs to the whole process: it is released when the process closes *any*
// descriptor of the file, and it never conflicts with the same process's other locks, so two
// subscribers in one process could not see each other's locks. An OFD lock belongs to one
// open() of the file and conflicts with every other open(), in this process or another.

// Returns true if this descriptor now holds the lock (or already did).
bool try_lock_byte(int fd, std::uint32_t byte);

void unlock_byte(int fd, std::uint32_t byte);

// True if another open file description holds a lock on `byte`. Locks held through `fd`
// itself never count.
bool is_byte_locked(int fd, std::uint32_t byte);

}  // namespace rtbus::detail
