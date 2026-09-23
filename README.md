# rtbus

A brokerless publish/subscribe middleware for real-time systems on Linux, written in C++17.

Processes publish messages on named topics. Subscribers on the same host receive them through
zero-copy shared memory; subscribers on other hosts receive them over UDP multicast. There is no
central broker to route through or to fail.

> **Status:** early development. The build, test and sanitizer setup is in place, along with the
> RAII wrappers for POSIX file descriptors and shared memory. The pub/sub API and transports are
> not implemented yet.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0 | Build system, sanitizers, CI, RAII wrappers for `fd` and shared memory | In progress |
| 1 | In-process pub/sub API: `Node`, `Publisher<T>`, `Subscriber<T>` | Planned |
| 2 | Lock-free shared-memory ring buffer, zero-copy loan API, futex wake-up | Planned |
| 3 | UDP transport, multicast discovery, fragmentation | Planned |
| 4 | QoS (best-effort, reliable, deadline), `SCHED_FIFO`, zero allocations on the hot path | Planned |
| 5 | Message code generator, CLI tools, end-to-end demo, benchmarks | Planned |

## Building

Requirements: Linux, GCC 10 or newer, CMake 3.21+, Ninja. GoogleTest is downloaded at configure
time.

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

Three presets are available, each building into its own `build/<preset>` directory:

| Preset | Purpose |
|---|---|
| `debug` | Plain debug build |
| `asan` | AddressSanitizer + UndefinedBehaviorSanitizer |
| `tsan` | ThreadSanitizer |

## Project layout

```
include/rtbus/   public headers
src/             library sources
tests/           GoogleTest suites
```
