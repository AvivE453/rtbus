#pragma once

#include <unistd.h>

#include <string>

namespace rtbus::test_support {

// ctest runs each test in its own process, so the pid keeps names unique across parallel runs.
inline std::string unique_shm_name() {
  static int counter = 0;
  return "/rtbus_test_" + std::to_string(::getpid()) + "_" + std::to_string(counter++);
}

}  // namespace rtbus::test_support
