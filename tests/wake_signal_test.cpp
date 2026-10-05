#include "rtbus/detail/wake_signal.hpp"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <new>
#include <thread>

#include "rtbus/shared_memory_region.hpp"
#include "unique_shm_name.hpp"

namespace rtbus::detail {
namespace {

using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

// Long enough that a test only reaches it when a wake-up was lost; the ctest TIMEOUT then
// fails the test instead of the test waiting for this.
constexpr auto kForever = std::chrono::hours(1);

// Waits until `flag` is set, following the protocol documented in wake_signal.hpp.
void wait_for(WakeSignal& signal, const std::atomic<bool>& flag) {
  while (!flag.load()) {
    const std::uint32_t ticket = signal.prepare_wait();
    if (flag.load()) {
      signal.cancel_wait();
    } else {
      signal.wait(ticket, kForever);
    }
  }
}

TEST(WakeSignalTest, WaitReturnsAfterTimeoutWithoutNotify) {
  WakeSignal signal;
  const auto start = Clock::now();

  signal.wait(signal.prepare_wait(), milliseconds(20));

  EXPECT_GE(Clock::now() - start, milliseconds(20));
}

// The race the protocol exists for, made deterministic: the notification lands after the
// waiter took its ticket but before it went to sleep. The kernel must refuse to sleep.
TEST(WakeSignalTest, NotifyBetweenPrepareAndWaitIsNotLost) {
  WakeSignal signal;
  const std::uint32_t ticket = signal.prepare_wait();
  signal.notify();
  const auto start = Clock::now();

  signal.wait(ticket, std::chrono::seconds(10));

  EXPECT_LT(Clock::now() - start, std::chrono::seconds(5));
}

TEST(WakeSignalTest, NotifyWakesASleepingThread) {
  WakeSignal signal;
  std::atomic<bool> flag{false};
  std::thread waiter([&] { wait_for(signal, flag); });

  std::this_thread::sleep_for(milliseconds(50));  // most likely asleep in the kernel by now
  flag.store(true);
  signal.notify();

  waiter.join();
}

// Two threads hand a turn back and forth thousands of times, each sleeping until it is its
// turn. A single lost wake-up leaves both asleep forever, and the ctest TIMEOUT fails the test.
TEST(WakeSignalTest, PingPongNeverLosesAWakeUp) {
  constexpr int kRounds = 5'000;
  WakeSignal ping_signal;
  WakeSignal pong_signal;
  std::atomic<int> turn{0};  // even: the ping thread's turn, odd: the pong thread's

  const auto wait_for_turn = [&](WakeSignal& signal, int parity, int round) {
    const auto my_turn = [&] { return turn.load() == 2 * round + parity; };
    while (!my_turn()) {
      const std::uint32_t ticket = signal.prepare_wait();
      if (my_turn()) {
        signal.cancel_wait();
      } else {
        signal.wait(ticket, kForever);
      }
    }
  };

  std::thread pong([&] {
    for (int round = 0; round < kRounds; ++round) {
      wait_for_turn(pong_signal, 1, round);
      turn.fetch_add(1);
      ping_signal.notify();
    }
  });
  for (int round = 0; round < kRounds; ++round) {
    wait_for_turn(ping_signal, 0, round);
    turn.fetch_add(1);
    pong_signal.notify();
  }
  pong.join();

  EXPECT_EQ(turn.load(), 2 * kRounds);
}

struct SharedSignal {
  WakeSignal signal;
  std::atomic<bool> flag{false};
  std::atomic<bool> child_ready{false};
};

// The futex must not be a process-private one: the waiter here is another process.
TEST(WakeSignalTest, NotifyWakesAWaiterInAnotherProcess) {
  const auto name = test_support::unique_shm_name();
  auto region = SharedMemoryRegion::create(name, sizeof(SharedSignal));
  auto* shared = new (region.data()) SharedSignal;

  const pid_t pid = ::fork();
  ASSERT_NE(pid, -1);
  if (pid == 0) {
    // Leave only via _exit, as in the SharedMemoryRegion fork test.
    int exit_code = 0;
    try {
      auto view = SharedMemoryRegion::open(name);
      auto* child_view = static_cast<SharedSignal*>(view.data());
      child_view->child_ready.store(true);
      wait_for(child_view->signal, child_view->flag);
    } catch (...) {
      exit_code = 1;
    }
    ::_exit(exit_code);
  }

  while (!shared->child_ready.load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(milliseconds(50));
  shared->flag.store(true);
  shared->signal.notify();

  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

}  // namespace
}  // namespace rtbus::detail
