# rtbus

A brokerless publish/subscribe middleware for real-time systems on Linux, written in C++17.

Processes publish messages on named topics. Subscribers on the same host receive them through
zero-copy shared memory; subscribers on other hosts receive them over UDP multicast. There is no
central broker to route through or to fail.

> **Status:** early development. The pub/sub API works within a single process; the
> shared-memory and UDP transports that carry messages between processes and hosts are next.

## Example

```cpp
#include "rtbus/node.hpp"

struct Pose {
  double x;
  double y;
};

rtbus::Node node("estimator");

// The callback runs on this subscriber's own thread.
auto subscriber = node.subscribe<Pose>("pose", [](const Pose& pose) { /* ... */ });

auto publisher = node.advertise<Pose>("pose");
publisher.publish(Pose{1.0, 2.0});  // never blocks
```

Each subscriber has a fixed-capacity queue and a dedicated thread, so a slow subscriber never
delays the publisher or other subscribers. When a subscriber's queue is full, the oldest message
is overwritten and counted in `subscriber.dropped()`. Message types must be trivially copyable
(plain structs), which is checked at compile time.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0 | Build system, sanitizers, CI, RAII wrappers for `fd` and shared memory | Done |
| 1 | In-process pub/sub API: `Node`, `Publisher<T>`, `Subscriber<T>` | Done |
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
