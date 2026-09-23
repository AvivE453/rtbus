#include "rtbus/file_descriptor.hpp"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <utility>

namespace rtbus {
namespace {

bool is_open(int fd) { return ::fcntl(fd, F_GETFD) != -1; }

// Returns the read end of a fresh pipe; the write end is closed right away.
int open_test_fd() {
  int fds[2];
  EXPECT_EQ(::pipe(fds), 0);
  ::close(fds[1]);
  return fds[0];
}

TEST(FileDescriptorTest, DefaultConstructedIsInvalid) {
  FileDescriptor fd;
  EXPECT_FALSE(fd.valid());
  EXPECT_EQ(fd.get(), -1);
}

TEST(FileDescriptorTest, ClosesOnDestruction) {
  const int raw = open_test_fd();
  {
    FileDescriptor fd(raw);
    EXPECT_TRUE(is_open(raw));
  }
  EXPECT_FALSE(is_open(raw));
}

TEST(FileDescriptorTest, MoveConstructionTransfersOwnership) {
  const int raw = open_test_fd();
  FileDescriptor source(raw);

  FileDescriptor target(std::move(source));

  // Moved-from state is part of the contract.
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_FALSE(source.valid());
  EXPECT_EQ(target.get(), raw);
  EXPECT_TRUE(is_open(raw));
}

TEST(FileDescriptorTest, MoveAssignmentClosesPreviousDescriptor) {
  const int first = open_test_fd();
  const int second = open_test_fd();
  FileDescriptor target(first);
  FileDescriptor source(second);

  target = std::move(source);

  EXPECT_FALSE(is_open(first));
  EXPECT_EQ(target.get(), second);
  // Moved-from state is part of the contract.
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_FALSE(source.valid());
}

TEST(FileDescriptorTest, ResetClosesAndAdoptsNewDescriptor) {
  const int first = open_test_fd();
  const int second = open_test_fd();
  FileDescriptor fd(first);

  fd.reset(second);

  EXPECT_FALSE(is_open(first));
  EXPECT_EQ(fd.get(), second);
}

}  // namespace
}  // namespace rtbus
