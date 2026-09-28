cmake_minimum_required(VERSION 3.21)
project(rtbus VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Linked PRIVATE into our own targets only, so -Werror never applies to
# third-party code pulled in by FetchContent.
add_library(rtbus_warnings INTERFACE)
target_compile_options(rtbus_warnings INTERFACE -Wall -Wextra -Wpedantic -Werror)

add_library(rtbus
  src/byte_lock.cpp
  src/file_descriptor.cpp
  src/segment.cpp
  src/segment_connection.cpp
  src/shared_memory_region.cpp
  src/shm_publisher.cpp
  src/shm_subscription.cpp
  src/wake_signal.cpp
)
add_library(rtbus::rtbus ALIAS rtbus)
target_include_directories(rtbus PUBLIC include)
# librt provides shm_open on glibc < 2.34 (Ubuntu 20.04 ships 2.31).
target_link_libraries(rtbus PRIVATE rtbus_warnings rt)

option(RTBUS_BUILD_TESTS "Build the rtbus test suite" ON)
if(RTBUS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
