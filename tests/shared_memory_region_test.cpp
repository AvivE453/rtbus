#include "rtbus/shared_memory_region.hpp"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <system_error>
#include <utility>

#include "unique_shm_name.hpp"

namespace rtbus {
namespace {

bool exists(const std::string& name) { return ::access(("/dev/shm" + name).c_str(), F_OK) == 0; }

std::uint8_t* bytes(const SharedMemoryRegion& region) {
  return static_cast<std::uint8_t*>(region.data());
}

TEST(SharedMemoryRegionTest, CreateMapsZeroedMemoryOfRequestedSize) {
  auto region = SharedMemoryRegion::create(test_support::unique_shm_name(), 4096);

  ASSERT_EQ(region.size(), 4096u);
  EXPECT_TRUE(std::all_of(bytes(region), bytes(region) + region.size(),
                          [](std::uint8_t b) { return b == 0; }));
}

TEST(SharedMemoryRegionTest, CreateFailsIfNameAlreadyExists) {
  const auto name = test_support::unique_shm_name();
  auto first = SharedMemoryRegion::create(name, 64);

  EXPECT_THROW(static_cast<void>(SharedMemoryRegion::create(name, 64)), std::system_error);
}

TEST(SharedMemoryRegionTest, OpenFailsIfNameDoesNotExist) {
  EXPECT_THROW(static_cast<void>(SharedMemoryRegion::open(test_support::unique_shm_name())),
               std::system_error);
}

TEST(SharedMemoryRegionTest, OpenedRegionSharesMemoryWithCreator) {
  const auto name = test_support::unique_shm_name();
  auto creator = SharedMemoryRegion::create(name, 64);
  auto opener = SharedMemoryRegion::open(name);

  bytes(creator)[10] = 42;

  EXPECT_EQ(opener.size(), 64u);
  EXPECT_NE(creator.data(), opener.data());
  EXPECT_EQ(bytes(opener)[10], 42);
}

TEST(SharedMemoryRegionTest, CreatorUnlinksNameOnDestruction) {
  const auto name = test_support::unique_shm_name();
  {
    auto region = SharedMemoryRegion::create(name, 64);
    EXPECT_TRUE(exists(name));
  }
  EXPECT_FALSE(exists(name));
}

TEST(SharedMemoryRegionTest, OpenerDoesNotUnlinkName) {
  const auto name = test_support::unique_shm_name();
  auto creator = SharedMemoryRegion::create(name, 64);
  { auto opener = SharedMemoryRegion::open(name); }

  EXPECT_TRUE(exists(name));
}

TEST(SharedMemoryRegionTest, MoveTransfersOwnershipOfName) {
  const auto name = test_support::unique_shm_name();
  auto source = SharedMemoryRegion::create(name, 64);
  {
    SharedMemoryRegion target(std::move(source));
    // Moved-from state is part of the contract.
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    EXPECT_EQ(source.data(), nullptr);
    EXPECT_EQ(source.fd(), -1);  // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    EXPECT_TRUE(exists(name));
  }
  EXPECT_FALSE(exists(name));
}

TEST(SharedMemoryRegionTest, RegionKeepsItsDescriptorOpen) {
  auto region = SharedMemoryRegion::create(test_support::unique_shm_name(), 4096);

  struct stat info {};
  ASSERT_EQ(::fstat(region.fd(), &info), 0);
  EXPECT_EQ(static_cast<std::size_t>(info.st_size), region.size());
}

TEST(SharedMemoryRegionTest, MapAdoptsDescriptorAndOwnership) {
  const auto name = test_support::unique_shm_name();
  FileDescriptor fd(::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600));
  ASSERT_TRUE(fd.valid());
  ASSERT_EQ(::ftruncate(fd.get(), 128), 0);
  const int raw_fd = fd.get();
  {
    auto region = SharedMemoryRegion::map(name, std::move(fd), /*owner=*/true);
    EXPECT_EQ(region.size(), 128u);
    EXPECT_EQ(region.fd(), raw_fd);
  }
  EXPECT_FALSE(exists(name));
}

TEST(SharedMemoryRegionTest, FailedMapByOwnerRemovesTheName) {
  const auto name = test_support::unique_shm_name();
  FileDescriptor fd(::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600));
  ASSERT_TRUE(fd.valid());

  // An empty object cannot be mapped.
  EXPECT_THROW(static_cast<void>(SharedMemoryRegion::map(name, std::move(fd), /*owner=*/true)),
               std::system_error);
  EXPECT_FALSE(exists(name));
}

// Stage 0 exit criterion: two separate processes see the same bytes. The child
// opens the region by name rather than using the mapping inherited from fork(),
// which is how unrelated processes will find each other.
TEST(SharedMemoryRegionTest, ForkedChildReadsParentWritesAndReplies) {
  constexpr char kRequest[] = "hello from parent";
  constexpr char kReply[] = "hello from child";
  constexpr std::size_t kReplyOffset = 1024;
  const auto name = test_support::unique_shm_name();
  auto region = SharedMemoryRegion::create(name, 4096);
  std::memcpy(region.data(), kRequest, sizeof(kRequest));

  const pid_t pid = ::fork();
  ASSERT_NE(pid, -1);
  if (pid == 0) {
    // The child is a copy of the whole test process. It must leave only via _exit:
    // returning or throwing would keep running gtest inside the child, and the copied
    // `region` destructor would unlink the parent's name.
    int exit_code = 0;
    try {
      auto view = SharedMemoryRegion::open(name);
      if (std::memcmp(view.data(), kRequest, sizeof(kRequest)) == 0) {
        std::memcpy(bytes(view) + kReplyOffset, kReply, sizeof(kReply));
      } else {
        exit_code = 1;
      }
    } catch (...) {
      exit_code = 2;
    }
    ::_exit(exit_code);
  }

  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0) << "child did not see the parent's bytes";
  EXPECT_EQ(std::memcmp(bytes(region) + kReplyOffset, kReply, sizeof(kReply)), 0);
}

}  // namespace
}  // namespace rtbus
