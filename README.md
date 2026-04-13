# High-Performance BGP Simulator (Ludicrous Edition)

A hyper-optimized, single-file C++ implementation of a Border Gateway Protocol (BGP) simulator for modeling Internet routing behavior. Designed for absolute maximum throughput and memory efficiency, this simulator placed 1st in competitive benchmarking, processing 78k+ ASes in under 0.8 seconds on just 2 CPU cores.

## Features & Optimizations

Unlike standard object-oriented approaches, this simulator utilizes a data-oriented design pattern to eliminate overhead:
- **Custom Memory Arena:** Utilizes a custom `malloc`-based arena to completely avoid `mmap` thrashing during high-volume I/O operations.
- **SIMD Acceleration:** Implements AVX2 intrinsics (`_mm256_loadu_si256`, `_mm256_cmpeq_epi32`) for rapid AS path loop detection.
- **Aggressive Pre-allocation:** Massive reservation of vectors (node degree * 64) to eliminate system time spent on dynamic memory reallocations.
- **Zero-Overhead I/O:** `ftruncate` pre-allocation for output files to prevent metadata update overhead during massive multi-threaded writes.
- **Lock-Free Concepts:** Utilizes aligned spinlocks (`AlignedSpinLock`) with hardware pause instructions to minimize thread contention.

## Quick Start

### Prerequisites
- C++ compiler with AVX2 support (GCC/Clang)
- Linux/WSL environment
- Pthreads library

### Building
To compile with full optimizations (AVX2 instructions required):
```bash
make
# Alternatively: g++ -O3 -march=native -pthread -Wall -Wextra -o bgp_simulator main.cpp
```

### Running
```bash
./bgp_simulator \
  --relationships bench/many/CAIDAASGraphCollector_2025.10.16.txt \
  --announcements bench/many/anns.csv \
  --rov-asns bench/many/rov_asns.csv
```

## Performance Benchmark

**Hardware:** WSL2 on Windows, Restricted to 2 CPU Cores
**Dataset:** 78,370 ASes, 40 announcements

| Metric | Time |
|-------|------|
| Average Execution Time | **0.788s** |
| Min Execution Time | 0.780s |
| Max Execution Time | 0.799s |

*Note: This implementation achieved ~2.6x faster execution times compared to standard multi-threaded, object-oriented OpenMP implementations on the same hardware.*

## Architecture 

To achieve sub-second processing times, this simulator foregoes standard `src/` and `include/` modularity in favor of a single compilation unit. This guarantees maximum compiler inlining and cache locality.

### Core Data Structures
- `FastVector<T>`: A custom, lightweight vector implementation replacing `std::vector` to remove exception-handling overhead and provide forced inlining.
- `PrivateOutArena`: Thread-local string building arenas that prevent I/O blocking until the final stage.
- `Announcement`: Packed 64-bit scoring system for ultra-fast route comparison.

### Propagation Model
1. **Up Phase**: Routes propagate from customers to providers using counting sort for optimal cache hits.
2. **Across Phase**: Routes propagate between peers.
3. **Down Phase**: Routes propagate from providers to customers.

## Input File Formats

### AS Relationships (CAIDA Format)
```
# Comments start with #
AS1|AS2|relationship|source
```
Where `relationship` is:
- `-1`: AS1 is provider to AS2 (customer-provider)
- `0`: AS1 and AS2 are peers