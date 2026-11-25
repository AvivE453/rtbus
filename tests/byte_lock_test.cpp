#include "rtbus/detail/byte_lock.hpp"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <new>
#include <thread>

#include "rtbus/shared_memory_region.hpp"
#include "unique_shm_name.hpp"

namespace rtbus::detail {
namespace {

// Two independent open()s of one object, as two processes (or two subscribers in one
// process) would have.
struct TwoDescriptors {
  SharedMemoryRegion first = SharedMemoryRegion::create(test_support::unique_shm_name(), 64);
  SharedMemoryRegion second = SharedMemoryRegion::open(first.name());
};

TEST(ByteLockTest, LockIsExclusiveBetweenOpenDescriptions) {
  TwoDescriptors files;

  EXPECT_TRUE(try_lock_byte(files.first.fd(), 3));
  EXPECT_FALSE(try_lock_byte(files.second.fd(), 3));
  EXPECT_TRUE(is_byte_locked(files.second.fd(), 3));
  EXPECT_FALSE(is_byte_locked(files.first.fd(), 3)) << "a descriptor's own lock never counts";
}

TEST(ByteLockTest, DifferentBytesAreIndependent) {
  TwoDescriptors files;

  EXPECT_TRUE(try_lock_byte(files.first.fd(), 0));
  EXPECT_TRUE(try_lock_byte(files.second.fd(), 1));
  EXPECT_FALSE(is_byte_locked(files.second.fd(), 2));
}

TEST(ByteLockTest, UnlockLetsAnotherDescriptionTakeTheLock) {
  TwoDescriptors files;
  ASSERT_TRUE(try_lock_byte(files.first.fd(), 5));

  unlock_byte(files.first.fd(), 5);

  EXPECT_TRUE(try_lock_byte(files.second.fd(), 5));
}

TEST(ByteLockTest, ClosingTheDescriptorReleasesItsLocks) {
  auto owner = SharedMemoryRegion::create(test_support::unique_shm_name(), 64);
  auto observer = SharedMemoryRegion::open(owner.name());
  {
    auto holder = SharedMemoryRegion::open(owner.name());
    ASSERT_TRUE(try_lock_byte(holder.fd(), 7));
    EXPECT_TRUE(is_byte_locked(observer.fd(), 7));
  }
  EXPECT_FALSE(is_byte_locked(observer.fd(), 7));
}

// The property the liveness checks rely on: kill -9 leaves no chance to clean up, yet the
// kernel still releases the dead process's locks.
TEST(ByteLockTest, KilledProcessLosesItsLock) {
  auto region = SharedMemoryRegion::create(test_support::unique_shm_name(), 64);
  auto* child_locked = new (region.data()) std::atomic<bool>(false);

  const pid_t pid = ::fork();
  ASSERT_NE(pid, -1);
  if (pid == 0) {
    auto own = SharedMemoryRegion::open(region.name());
    if (try_lock_byte(own.fd(), 2)) {
      child_locked->store(true);
    }
    while (true) {
      ::pause();  // wait to be killed
    }
  }

  while (!child_locked->load()) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(is_byte_locked(region.fd(), 2));

  ASSERT_EQ(::kill(pid, SIGKILL), 0);
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);

  EXPECT_FALSE(is_byte_locked(region.fd(), 2));
}

}  // namespace
}  // namespace rtbus::detail
