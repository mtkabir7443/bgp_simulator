# BGP Simulator

A C++17 CPU engine and experimental CUDA engine for studying static inter-AS
route selection, commercial routing policy, and simplified Route Origin
Validation (ROV). A pybind11 module exposes the CPU engine to Python.

The focus is correctness, reproducible measurement, and a compact implementation.
This is a static routing model: it does not simulate BGP sessions, update timers,
withdrawals, or convergence over time. Read the [routing model](docs/model.md)
before interpreting results.

## Quick start

Run from the repository root in Linux or WSL2. The CPU needs Make and a C++17
compiler such as g++.

```bash
make demo
```

This builds the CPU and runs the included disconnected-topology example. Inspect
`outputs/demo/cpu/ribs.csv`: AS1 and AS2 receive routes; AS3 and AS4 do not.

For the Python bindings, benchmarks, and tests:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
make python
make benchmark
```

Activate `.venv` in each new terminal. Python development headers are required;
on Ubuntu, install `python3-dev` and `python3-venv` if they are missing.
See [development](docs/development.md) for testing and measurements.

## Everyday commands

| Command | Result |
| --- | --- |
| `make cpu` | Build `build/bgp_simulator` |
| `make gpu` | Build `build/bgp_sim_gpu` |
| `make python` | Build the Python extension in `build/` |
| `make` | Build CPU, GPU, and Python extension |
| `make demo` / `make demo-gpu` | Run a small example under `outputs/demo/` |
| `make pytest` | Build all components and run the Python suites |
| `make test` | Run the separate GoogleTest memory experiments |
| `make benchmark` / `make benchmark-synthetic` | Run small CPU benchmarks |
| `make clean` | Remove build products; preserve datasets and results |
| `make help` | Show available targets |

GPU builds require the CUDA toolkit; execution needs a compatible NVIDIA GPU and
driver. The default target is `sm_86`. Override it with
`make gpu GPU_ARCH=sm_89`, for example. Use `make -B gpu GPU_ARCH=sm_89` when
changing the target of an existing binary.

## Use your own data

Both engines write `ribs.csv` in their current working directory. Keep runs in
separate output directories; repeating a run in the same directory replaces
that result. For inputs stored in `data/generated/`:

```bash
make cpu
mkdir -p outputs/my-run
(
    cd outputs/my-run
    ../../build/bgp_simulator \
        --relationships ../../data/generated/rel.txt \
        --announcements ../../data/generated/ann.txt
)
```

Use `build/bgp_sim_gpu` for GPU execution. ROV filtering is optional: add
`--rov-asns <path>` only when you have a file listing filtering ASNs.
See [input formats, output, and the Python API](docs/model.md#input-and-output).
Large datasets can exceed GPU memory; the included demos are the starting point.

## Repository map

```text
src/                 CPU and CUDA engines
tests/               Python suites, C++ memory experiments, and fixtures/
benchmarks/          CPU measurement harnesses
scripts/             Data generation, stress testing, and output comparison
experiments/         Exploratory ROV, hijack, and prefix-lookup demonstrations
docs/                Model, development notes, and illustration assets
data/archive/        Preserved historical output
data/generated/      Local generated inputs (ignored)
build/               Binaries and Python extension (ignored)
outputs/             Local run results and generated figures (ignored)
```

[routing model](docs/model.md) · [development and roadmap](docs/development.md)
