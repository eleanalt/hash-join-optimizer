# Hash Join Optimizer

A high-performance in-memory hash join pipeline built in C++, developed as part of the [SIGMOD 2025 Programming Contest](https://sigmod-contest-2025.github.io). The project explores advanced database internals — from custom hash table designs to parallel execution — achieving a **>16× speedup** over the baseline implementation.

---

## Overview

Given a join pipeline and pre-filtered columnar input data, the system efficiently executes multi-way hash joins over in-memory relational tables. The implementation progressively introduces optimizations across three stages:

1. **Custom Hash Tables** — Robin Hood, Cuckoo, and Hopscotch hashing replacing `std::unordered_map`
2. **Late Materialization & Column-Store Layout** — deferred string handling, cache-friendly intermediate results
3. **Parallel Execution** — partitioned build/probe phases, slab-based memory allocation, and work-stealing

---

## Performance Results

| Stage | Hash Table | Execution Time |
|---|---|---|
| Baseline | `std::unordered_map` | 340,000 ms |
| Phase 1 | Cuckoo Hashing | 330,000 ms |
| Phase 1 | Unchained Hashtable | 240,000 ms |
| Phase 2 | Unchained + Late Materialization | 37,000 ms |
| Phase 3 | Unchained + Parallelism (8 threads) | **15,000 ms** |

> Tested on: Intel Core i7, 16GB RAM, Ubuntu 24.04

---

## Key Techniques

### Hash Table Implementations

**Robin Hood Hashing**
Open-addressing with linear probing and PSL-based displacement ("stealing"). Uses `std::hash` + Murmur3 finalizer. Reduced maximum probe sequence length by up to 95%. Load factor: 0.9.

**Cuckoo Hashing**
Dual-table design with two independent hash functions (Murmur-like + xorshift). Guarantees O(1) lookups by checking at most two positions. Load factor: 0.5 to prevent displacement cycles.

**Hopscotch Hashing**
Neighborhood-based open addressing with a 32-bit bitmap per bucket (H=32). Keeps keys within a cache-local neighborhood. Load factor: 0.9.

**Unchained Hashtable** *(best performer)*
Directory-based layout with contiguous adjacency array storage, sorted by hash prefix. Each directory entry embeds a 16-bit Bloom filter (packed into unused pointer bits) for fast early rejection of non-matching probes. Uses Fibonacci hashing (outperformed CRC32 by ~13%).

### Late Materialization & String Handling

Introduced a compact 64-bit `StrRef` type encoding `(table_id, column_id, page_id, offset)` — avoiding string copies during intermediate join steps. VARCHAR columns are only fully materialized at the final output stage. A unified `value_t` type encodes INT32, StrRef, or NULL using tag bits, eliminating `std::variant` overhead.

### Column-Store Intermediate Results

Replaced row-oriented intermediate buffers with a paged column structure (`column_t`), storing `value_t` values contiguously per column. Dense INT32 columns without NULLs bypass copying entirely via direct pointer access into input pages.

### Parallel Execution

- **Partitioned Build**: Tuples distributed into thread-local partition buffers using high bits of hash value. No synchronization during collection.
- **Three-Level Slab Allocator**: Global → per-thread chunks → per-partition bump allocators. Reduces allocator contention and keeps tuples within the same partition largely contiguous.
- **Parallel Probing**: Threads independently probe disjoint partition ranges. Read-only tuple storage requires no synchronization.
- **Work Stealing**: Idle threads claim remaining probe pages via an atomic counter, improving load balance.

| Threads | Execution Time |
|---|---|
| 2 | 25,000 ms |
| 4 | 17,000 ms |
| 8 | 15,000 ms |

---

## Build & Run

```bash
# Download dataset
./download_imdb.sh

# Build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc)

# Build query cache (faster subsequent runs)
./build/build_cache plans.json

# Run with a specific hash algorithm
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev -DHASH_ALGO=unchained
cmake --build build --target fast

# Run queries
./build/fast plans.json
```

Available `HASH_ALGO` values: `robinhood`, `cuckoo`, `hopscotch`, `unchained`

---

## Unit Tests

```bash
cmake --build build -- -j $(nproc) robinhood_tests
cmake --build build -- -j $(nproc) cuckoo_tests
cmake --build build -- -j $(nproc) hopscotch_tests
cmake --build build -- -j $(nproc) unchained_tests
```

Correctness is also validated automatically on every push via **GitHub Actions**.

---

## Tech Stack

- **Language**: C++
- **Build System**: CMake
- **Compiler**: Clang 18 / GCC ≥ 9.4
- **Platform**: Linux
- **Testing**: Custom unit test suites + GitHub Actions CI
- **Dataset**: IMDB (JOB benchmark)

---

Eleana Lyti 
