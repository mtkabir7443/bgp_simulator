# BGP Simulator

A C++17 CPU engine and experimental CUDA engine for studying static inter-AS route
selection, commercial routing policy, and simplified Route Origin Validation
(ROV). A PyBind11 module exposes the CPU engine to Python.

The project emphasizes explicit model assumptions, expected-route tests, and
reproducible measurements. It does not implement the complete BGP protocol or
simulate sessions, update timers, withdrawals, or convergence over time. See
[RFC 4271](https://www.rfc-editor.org/rfc/rfc4271.html) for the protocol and
[RFC 6811](https://www.rfc-editor.org/rfc/rfc6811.html) for origin validation.

## Routing model

- Routes travel upward to providers, across at most one peer link, then downward
  to customers. The provider/customer hierarchy must be acyclic; both engines
  reject cycles. Peer links may form cycles.
- Selection prefers local origin, then customer, peer, and provider routes.
  Ties use shorter AS paths, then the lower next-hop ASN.
- Paths include the observing AS and are limited to 16 ASNs. Routes exceeding
  that limit are omitted; this is a model limit, not a BGP protocol limit.
- ROV uses a supplied invalid flag and a set of filtering ASes. Invalid routes
  are rejected at those ASes, including locally seeded announcements. This is an
  experimental filtering rule; the engine does not validate ROAs or implement
  the full Valid/Invalid/NotFound state model.
- Prefixes are matched by input strings. Supply canonical IPv4 CIDRs and origins
  present in the relationship file; unknown origins are currently ignored.
  Conflicting duplicate announcements are not a supported contract.

## Build

Linux or WSL2, g++ with C++17, and Make are required for the CPU engine. CUDA
requires an NVIDIA GPU and compatible toolkit/driver. Bindings require Python
development headers and `pybind11`; Python tests require `pytest`.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pybind11 pytest
make                     # CPU binary, GPU binary, and Python extension
```

Run these commands from the repository directory. Activate the environment with
`source .venv/bin/activate` in each new terminal before building the Python
extension or running Python tests. If Ubuntu reports that `venv` is unavailable,
install `python3-venv` with `sudo apt install python3-venv`, then retry creation.

To build an individual component instead of all three:

```bash
make cpu                 # bgp_simulator
make gpu                 # bgp_sim_gpu
make python              # bgp_simulator*.so
```

The default GPU architecture is `sm_86`. To select another architecture supported
by your GPU and CUDA toolkit, use an override when building, for example
`make gpu GPU_ARCH=sm_89`. If the GPU binary already exists, force recompilation
with `make -B gpu GPU_ARCH=sm_89`; Make does not track changes to this variable.

Production code does not request huge pages. Reserving system huge pages is not
required to run either engine.

## Run

```bash
./bgp_simulator --relationships rel.txt --announcements ann.txt
./bgp_sim_gpu --relationships rel.txt --announcements ann.txt
```

Those commands use your current datasets, which may exceed GPU memory. For a
small GPU example using the included disconnected-topology fixture:

```bash
./bgp_sim_gpu --relationships rel_island.txt --announcements ann_island.txt
```

This fixture produces two routes, at AS1 and AS2. AS3 and AS4 are disconnected
from the origin and receive no route. `make pytest` runs its GPU fixtures in
temporary directories if you want a check that preserves existing output.

Both engines write `ribs.csv` in the current directory. Use a separate working
directory for each run if previous output matters. Missing requested files and
provider/customer cycles fail with diagnostics and nonzero CLI exit status;
Python calls raise an exception. Readable empty input is allowed.

Relationship rows use `provider|customer|-1` or `peer|peer|0`:

```text
1|2|0
2|3|-1
```

Announcement rows use `origin_asn,prefix[,rov_invalid]`. The optional third field
is a boolean flag, not a timestamp. Use `0` for valid and `1` for invalid:

```text
1,192.0.2.0/24,0
```

To enable ROV filtering, create a file containing one filtering ASN per line and
add `--rov-asns <path-to-file>` to either command. Omit this option when no ROV
file is available. The example above produces:

```csv
asn,prefix,as_path
1,192.0.2.0/24,"(1,)"
2,192.0.2.0/24,"(2, 1)"
3,192.0.2.0/24,"(3, 2, 1)"
```

Row order is not part of the interface. Tests compare route contents, not bytes.
The Python API uses the same files and output:

```python
import bgp_simulator
bgp_simulator.run(relationships="rel.txt", announcements="ann.txt", rov_asns="")
```

Calls are sequential and reset route state between runs. The binding uses global
engine state and writes to the process's current directory.

## Test

```bash
make pytest   # build CPU, GPU, binding; run the Python suites
make test     # legacy GoogleTest memory experiments (requires libgtest-dev)
```

`test_routing.py` specifies expected routes for policy preference, path length,
ASN ties and reordered input, peer export restrictions, disconnected components,
ROV, and 32-bit ASNs. It also tests the 16-ASN boundary, rejected cycles, missing
files, and page-boundary input without a final newline. `test_pipeline.py`
retains the original small CPU/GPU parity checks. `test_python_binding.py`
checks execution and recovery after rejection. `test_benchmarks.py` checks
workload generation, actual route counts, and preservation of existing files.

The GoogleTest file contains a separate allocator/RIB experiment. Passing it
does not establish production allocator safety. GPU tests require a working
CUDA device; the routing suite skips a backend if its binary is absent.

## Measure

Start with small, isolated runs:

```bash
python3 benchmark.py --routes 10 100
python3 caida_benchmark.py --tier1 2 --tier2 4 --stubs 20 --prefixes 8
```

Both scripts use temporary working directories and count actual output RIB rows.
Engine elapsed time includes parsing, route selection, and CSV serialization;
dataset generation and output counting are outside that timer. These are
single-run CPU measurements, not convergence times or established GPU speedups.
Record hardware, revision, workload, repeated samples, and peak memory before
making performance claims.

Despite its historical filename, `caida_benchmark.py` generates a **synthetic
tiered graph**, not a measured CAIDA dataset. Earlier 78k-AS/0.8-second and
competitive-ranking claims lack a reproducible result bundle here and are not
current performance guarantees. Its corrected default really generates 500
unique prefixes; the earlier generator produced only two, a much lighter load.

## Architecture and limits

### Archived output

`ribs_cpu.csv.gz` preserves an existing CPU output snapshot as a lossless gzip
archive. It predates this correctness pass and is not a validated benchmark or
expected-results fixture. To restore it when the local CSV is absent, run
`gzip -dk ribs_cpu.csv.gz`. The uncompressed `ribs_cpu.csv` is ignored by Git;
the archive preserves the data without committing an oversized individual file.

### Engine storage

The CPU uses flattened relationship arrays, per-AS vectors of route records,
parallel announcement parsing, staged worker dispatch, and mmap input/output.
Its aligned route record occupies 128 bytes. The GPU uses relationship-typed
adjacency and dense route arrays, with atomic score selection followed by path
materialization. Scores use raw ASNs for consistent tie-breaking.

GPU RIB storage alone costs `AS_count * unique_prefix_count * 78` bytes, plus
graph, staging, and host memory. A free-VRAM check rejects RIBs exceeding device
memory; it is not a complete resource guarantee. GPU stage timing excludes host
parsing and final CSV output. Large GPU workloads are not yet validated.

Remaining work includes strict shared input validation, defined duplicate
handling, dynamic collision-safe prefix storage, checked allocation/output
errors, and wider differential testing. The CPU preallocates large buffers per
hardware thread; measure these costs before tuning concurrency.

`lpm_churn_sim.py`, `simulate_internet_rov.py`, and `visualize_hijack.py` are
exploratory demonstrations, not validated research pipelines. Known issues
include the LPM helper's /32 lookup, ROV prefix generation and outcome counting,
and an illustrative graph not derived from engine output. `stress_test.py` and
the data generators write into their current directory; run them in scratch space.

## Development order

1. Expand correctness tests with an independent small-graph oracle; finish input
   validation and resource error handling.
2. Establish reproducible CPU measurements with actual counts, repeated runs,
   dataset identity, and peak memory.
3. Measure GPU memory/runtime across node and prefix counts. Compare bounded
   prefix batching against sparse RIB storage before choosing a redesign.
4. Validate one ROV/hijack experiment with canonical prefixes, fixed seeds, exact
   origin classification, and visualization of actual output.
