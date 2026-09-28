include(FetchContent)

FetchContent_Declare(googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
  URL_HASH SHA256=7b42b4d6ed48810c5362c265a17faebe90dc2373c885e5216439d37927f02926
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

add_executable(rtbus_tests
  byte_lock_test.cpp
  chunk_pool_test.cpp
  file_descriptor_test.cpp
  index_ring_test.cpp
  message_type_test.cpp
  node_test.cpp
  segment_test.cpp
  shared_memory_region_test.cpp
  shm_transport_test.cpp
  wake_signal_test.cpp
)
target_link_libraries(rtbus_tests PRIVATE rtbus::rtbus rtbus_warnings GTest::gtest_main)
# A test that deadlocks fails after 30 s instead of hanging the run.
gtest_discover_tests(rtbus_tests PROPERTIES TIMEOUT 30)
