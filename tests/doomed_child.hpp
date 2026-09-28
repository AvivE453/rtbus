#pragma once

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <functional>
#include <system_error>

#include "rtbus/file_descriptor.hpp"

namespace rtbus::test_support {

// Blocks the calling thread until the process is killed.
[[noreturn]] inline void wait_to_be_killed() {
  for (;;) {
    ::pause();
  }
}

// A forked process that plays its part in a test and then dies the way a crash ends it:
// SIGKILL, so no destructor runs and nothing is unlocked or unlinked on the way out.
//
// Fork only while the test is single-threaded. The child gets only the forking thread, so a
// lock that another thread held at that moment would stay locked in the child forever.
class DoomedChild {
 public:
  // Forks and runs `body` in the child. The body ends in wait_to_be_killed(); if it throws
  // instead, the child exits and the parent's next wait_for_report() returns false.
  explicit DoomedChild(const std::function<void(DoomedChild&)>& body) {
    int ends[2];
    if (::pipe(ends) != 0) {
      throw std::system_error(errno, std::generic_category(), "pipe");
    }
    read_end_.reset(ends[0]);
    write_end_.reset(ends[1]);
    pid_ = ::fork();
    if (pid_ == -1) {
      throw std::system_error(errno, std::generic_category(), "fork");
    }
    if (pid_ == 0) {
      // Leave only via _exit: returning would run the rest of the parent's test here.
      read_end_.reset();
      try {
        body(*this);
      } catch (...) {
        ::_exit(1);
      }
      ::_exit(1);  // the body returned instead of waiting to be killed, e.g. a failed ASSERT
    }
    write_end_.reset();
  }

  ~DoomedChild() { kill(); }

  DoomedChild(const DoomedChild&) = delete;
  DoomedChild& operator=(const DoomedChild&) = delete;
  DoomedChild(DoomedChild&&) = delete;
  DoomedChild& operator=(DoomedChild&&) = delete;

  // Child: tells the parent that the body reached its next step.
  void report() const {
    const char step = 1;
    if (::write(write_end_.get(), &step, 1) != 1) {
      ::_exit(1);
    }
  }

  // Parent: waits for the child's next report. False if the child exited instead.
  [[nodiscard]] bool wait_for_report() const {
    char step = 0;
    return ::read(read_end_.get(), &step, 1) == 1;
  }

  // Parent: kills the child and reaps it. Once it is reaped, the kernel has closed its
  // descriptors and released its locks.
  void kill() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      int status = 0;
      ::waitpid(pid_, &status, 0);
      pid_ = -1;
    }
  }

 private:
  FileDescriptor read_end_;
  FileDescriptor write_end_;
  pid_t pid_ = -1;
};

}  // namespace rtbus::test_support
