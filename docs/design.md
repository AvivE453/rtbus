# How rtbus works

## The shared-memory transport

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

## Limits

- **Topics:** one publisher and at most 8 subscribers per topic.
- **Loans:** at most 2 outstanding loans per publisher.
- **Message types:** trivially copyable, default-constructible structs, aligned to at most 64
  bytes. Both sides must see the same type name, size and alignment. Subscribing to a publisher
  of another type throws, and a subscriber waiting for its publisher ignores one of another
  type.

## Build presets

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

How to reproduce the benchmarks is in [bench/RESULTS.md](../bench/RESULTS.md).

## Project layout

```
include/rtbus/          public headers
include/rtbus/detail/   the transport's internals
src/                    library sources
tests/                  GoogleTest suites and the stress test
bench/                  latency harness, microbenchmarks and results
docs/                   this file
```
