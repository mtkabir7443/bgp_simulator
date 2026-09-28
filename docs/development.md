# Development

[Back to README](../README.md) · [Routing model](model.md)

Keep source, tests, input datasets, and generated results separate. Commands below
assume the repository root and an activated `.venv`. Build artifacts live in
`build/`; generated inputs and results are ignored by Git.

## Build and test

```bash
make cpu python
make pytest
make test
```

`make pytest` builds the CPU binary, CUDA binary, and Python extension before
running the Python suites, so it requires the CUDA toolkit. GPU execution also
requires a working CUDA device. For CPU and binding development without CUDA:

```bash
make cpu python
python -m pytest -k "not gpu"
```

The GoogleTest target requires `libgtest-dev`. It contains a separate allocator
and RIB experiment; passing it does not establish production allocator safety.

| Suite | Purpose |
| --- | --- |
| `tests/test_routing.py` | Expected routes for policy preference, path length, ASN ties, peer export, disconnected components, ROV, and 32-bit ASNs; boundary and rejection checks |
| `tests/test_pipeline.py` | Binary availability, minimal CPU/GPU execution, and output parity |
| `tests/test_python_binding.py` | Binding execution and recovery after rejection |
| `tests/test_benchmarks.py` | Workload generation, actual route counts, and preservation of existing files |
| `tests/test_memory.cpp` | Legacy GoogleTest allocator/RIB experiments |

The routing tests also exercise the 16-ASN boundary, reordered inputs, rejected
cycles, missing files, and page-boundary input without a final newline.
`tests/fixtures/` holds the small checked-in empty, island, and cycle inputs.
Generated datasets belong in `data/generated/`, not in the fixture directory.

`make clean` removes build products without removing datasets or results.
Changing compiler flags or `GPU_ARCH` does not invalidate an existing binary;
use `make -B <target>` to force recompilation when changing those options.
Production code does not request huge pages; no huge-page reservation is needed.

## Measurements

Start with small CPU workloads:

```bash
make benchmark
make benchmark-synthetic
```

Override a target's workload with `ARGS`, for example
`make benchmark ARGS="--routes 100 1000"`. The scripts also accept explicit sizes:

```bash
python benchmarks/benchmark.py --routes 10 100
python benchmarks/caida_benchmark.py --tier1 2 --tier2 4 --stubs 20 --prefixes 8
```

Both scripts find the extension in `build/`, use temporary working directories,
and count actual output RIB rows. Engine elapsed time includes input parsing,
route selection, and CSV serialization. Dataset generation and output counting
are outside the timer. These are single-run CPU measurements, not convergence
times or established GPU speedups. Record hardware, revision, workload, repeated
samples, and peak memory before making performance claims.

Despite its historical filename, `caida_benchmark.py` generates a **synthetic
tiered graph**, not a measured CAIDA dataset. Earlier 78k-AS/0.8-second and
competitive-ranking claims lack a reproducible result bundle here and are not
current performance guarantees. Its corrected default generates 500 unique
prefixes; the earlier generator produced only two, a much lighter load.

## Utilities and experiments

| Tool | Behavior |
| --- | --- |
| `scripts/generate_data.py` | Generate a large synthetic dataset in `data/generated/`; use `--output-dir PATH` to choose a destination |
| `scripts/generate_edge_cases.py` | Generate island and rejected-cycle examples in `data/generated/`; also accepts `--output-dir PATH` |
| `scripts/stress_test.py --routes 100` | Run the CPU binary against a generated workload in a temporary directory; default is 500,000 announcements |
| `scripts/compare_output.sh <expected-file> <actual-file>` | Compare sorted outputs, ignoring whitespace |
| `experiments/lpm_churn_sim.py` | Standalone illustrative prefix-lookup demonstration |
| `experiments/simulate_internet_rov.py` | Exploratory ROV simulation using temporary input/output files |
| `experiments/visualize_hijack.py` | Draw an illustrative graph in `outputs/experiments/`; accepts `--output PATH` |

Run Python tools with `python <path-to-script>`. The stress test needs `make cpu`;
the ROV experiment needs `make python`. Visualization additionally requires
`networkx` and `matplotlib`; install these optional dependencies with
`python -m pip install networkx matplotlib` before running the visual demo.

The experiments are not validated research pipelines. Known issues include the
LPM helper's /32 lookup, ROV prefix generation and outcome counting, and an
illustrative graph that is not derived from engine output. The reference image
in [docs/assets](assets/hijack_attack_graph.png) is preserved as an illustration.
Moving these tools into `experiments/` does not validate their algorithms.

## Architecture and resource limits

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

## Preserved data

`data/archive/ribs_cpu.csv.gz` is a lossless archive of a historical CPU result.
It predates the correctness pass and is not a validated benchmark or an
expected-results fixture. Preserve it as historical data; the raw local CSV lives
under ignored `outputs/previous/`.

To extract a separate copy without changing the archive:

```bash
mkdir -p outputs/archive
gzip -dc data/archive/ribs_cpu.csv.gz > outputs/archive/ribs_cpu.csv
```

This replaces an existing `outputs/archive/ribs_cpu.csv`. Local generated inputs
live under `data/generated/`; any preserved ad hoc benchmark inputs live under
ignored `data/bench/`. Tests and benchmark harnesses create disposable workspaces.

## Development order

1. Expand correctness tests with an independent small-graph oracle; finish input
   validation and resource error handling.
2. Establish reproducible CPU measurements with actual counts, repeated runs,
   dataset identity, and peak memory.
3. Measure GPU memory/runtime across node and prefix counts. Compare bounded
   prefix batching against sparse RIB storage before choosing a redesign.
4. Validate one ROV/hijack experiment with canonical prefixes, fixed seeds, exact
   origin classification, and visualization of actual output.
