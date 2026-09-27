#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <vector>

namespace rtbus::test_support {

// Collects values from a subscriber thread and lets the test wait for them.
// It waits without a timeout on purpose: GCC 10's ThreadSanitizer does not intercept the
// pthread_cond_clockwait call behind condition_variable::wait_for, and reports a false
// "double lock". A hang still fails the test through the ctest TIMEOUT instead.
template <typename T>
class Collector {
 public:
  void add(const T& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    values_.push_back(value);
    changed_.notify_all();
  }

  std::vector<T> wait_until_count(std::size_t count) {
    std::unique_lock<std::mutex> lock(mutex_);
    changed_.wait(lock, [&] { return values_.size() >= count; });
    return values_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  std::vector<T> values_;
};

}  // namespace rtbus::test_support
