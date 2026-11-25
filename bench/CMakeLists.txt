include(FetchContent)

FetchContent_Declare(benchmark
  URL https://github.com/google/benchmark/archive/refs/tags/v1.9.1.tar.gz
  URL_HASH SHA256=32131c08ee31eeff2c8968d7e874f3cb648034377dfc32a4c377fa8796d84981
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(benchmark)

# End-to-end latency between two processes: rtbus against a Unix domain socket.
add_executable(rtbus_latency latency.cpp)
target_link_libraries(rtbus_latency PRIVATE rtbus::rtbus rtbus_warnings)

# The cost of each step on the publish path, one thread, no wake-ups.
add_executable(rtbus_micro micro.cpp)
target_link_libraries(rtbus_micro PRIVATE rtbus::rtbus rtbus_warnings benchmark::benchmark_main)
