# rtbus

A brokerless publish/subscribe middleware for real-time systems on Linux, written in C++17.

Processes publish messages on named topics. Subscribers on the same host receive them through
zero-copy shared memory; subscribers on other hosts will receive them over UDP multicast. There
is no central broker to route through or to fail.

> **Status:** the shared-memory transport works between processes on one host. The UDP
> transport for other hosts is next.

## Example

```cpp
#include "rtbus/node.hpp"

struct Pose {
  double x;
  double y;
};

// Process A
rtbus::Node estimator("estimator");
auto publisher = estimator.advertise<Pose>("pose");

auto pose = publisher.loan();  // a message in shared memory, filled in place
pose->x = 1.0;
pose->y = 2.0;
publisher.publish(std::move(pose));  // nothing is copied, and nobody is waited for

publisher.publish(Pose{3.0, 4.0});  // or copy a message in

// Process B
rtbus::Node planner("planner");
auto subscriber = planner.subscribe<Pose>(
    "pose", [](const Pose& pose) { /* runs on this subscriber's own thread */ });
```

A subscriber can start before or after its publisher, and it reconnects on its own when the
publisher is restarted or replaced.

## How it works

- **One shared-memory segment per topic.** The publisher creates `/dev/shm/rtbus.<topic>`. The
  segment holds a pool of message chunks and one slot for each subscriber.
- **Zero-copy with reference counts.** A published chunk is shared by every subscriber that
  receives it. It returns to the pool when the last one finishes with it.
- **A lock-free queue per subscriber.** Each subscriber has its own queue of chunk indices, and
  the publisher never waits for it. A subscriber that falls behind loses its oldest queued
  messages (KeepLast), counted in `subscriber.dropped()`. Its queue holds 1 to 16 messages
  (`SubscriberOptions::queue_capacity`).
- **Futex wake-up.** An idle subscriber sleeps in the kernel until a message arrives. The
  futex is skipped entirely when nobody is asleep. A subscriber that needs the lowest latency
  can busy-poll instead (`WaitMode::kBusyPoll`), at the price of a CPU core.
- **Crash recovery.** Every participant holds a byte-range lock on the segment file, and the
  kernel releases it when the process dies, even by `kill -9`. From those locks:
  - Subscribers notice a dead publisher.
  - A new publisher takes over the dead one's segment.
  - The slot and chunks of a dead subscriber are taken back, including a message it was
    reading when it died.

### Limits

- **Topics:** one publisher and at most 8 subscribers per topic.
- **Loans:** at most 2 outstanding loans per publisher.
- **Message types:** trivially copyable, default-constructible structs, aligned to at most 64
  bytes. Both sides must see the same type name, size and alignment. Subscribing to a publisher
  of another type throws, and a subscriber waiting for its publisher ignores one of another
  type.

## Performance

Measured on a laptop (Core i7-11390H, no real-time tuning), one message per millisecond between
two processes. See [bench/RESULTS.md](bench/RESULTS.md) for the method, every percentile and the
caveats.

| p50 one-way latency | 64 B | 1 MB |
|---|---:|---:|
| Unix domain socket | 3.5 µs | 167 µs |
| rtbus, sleeping subscriber | 2.5 µs | 3.1 µs |
| rtbus, busy-polling subscriber | 0.4 µs | 0.4 µs |

The two top rows were measured with CPU idle states kept out. With them, about a quarter of
the messages to a sleeping receiver on this laptop wait around 100 µs for a CPU core to wake.
The busy-polling row comes from a run with idle states left on, because a busy-polling
subscriber needs a core of its own. Publishing and taking a message costs about 72 ns of CPU at
any size up to 1 MB.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0 | Build system, sanitizers, CI, RAII wrappers for `fd` and shared memory | Done |
| 1 | Pub/sub API: `Node`, `Publisher<T>`, `Subscriber<T>` | Done |
| 2 | Shared-memory transport: lock-free queues, zero-copy loans, futex wake-up, crash recovery, latency benchmarks | Done |
| 3 | UDP transport, multicast discovery, fragmentation | Planned |
| 4 | QoS (best-effort, reliable, deadline), `SCHED_FIFO`, zero allocations on the hot path | Planned |
| 5 | Message code generator, CLI tools, end-to-end demo | Planned |

## Building

Requirements: Linux, GCC 10 or newer, CMake 3.21+, Ninja. GoogleTest and Google Benchmark are
downloaded at configure time.

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

Each preset builds into its own `build/<preset>` directory:

| Preset | Purpose |
|---|---|
| `debug` | Plain debug build |
| `asan` | AddressSanitizer + UndefinedBehaviorSanitizer |
| `tsan` | ThreadSanitizer |
| `release` | Optimized build of the benchmarks only |

The tests include a randomized stress test (`Stress.Short`, 5 seconds). Its long form restarts
publishers, churns subscribers and kills subscriber processes for ten minutes:

```bash
./build/tsan/tests/rtbus_stress 600
```

To reproduce the benchmarks:

```bash
cmake --preset release && cmake --build --preset release
./build/release/bench/rtbus_latency 10000 latency.csv
python3 bench/plot_latency.py latency.csv latency.png
./build/release/bench/rtbus_micro
```

## Project layout

```
include/rtbus/          public headers
include/rtbus/detail/   the transport's internals
src/                    library sources
tests/                  GoogleTest suites and the stress test
bench/                  latency harness, microbenchmarks and results
```
