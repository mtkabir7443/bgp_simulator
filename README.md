# BGP Simulator (Singularity Edition)

A high-performance, heterogeneous CPU/GPU BGP (Border Gateway Protocol) routing simulator written in C++ and CUDA. Designed to test and simulate global-scale internet routing topographies, route propagation, and loop detection under extreme memory constraints.

## 🚀 Performance Highlights

- **GPU Acceleration:** Offloads parallel graph traversal and route evaluation to Nvidia streaming multiprocessors using custom CUDA kernels.
- **Struct-of-Arrays (SoA) Memory Layout:** Replaces standard object arrays with continuous, cache-aligned arrays to maximize memory throughput and eliminate cache thrashing.
- **Kernel-Level Huge Pages (`MAP_HUGETLB`):** Bypasses standard virtual memory translation overhead, completely eliminating Translation Lookaside Buffer (TLB) misses during massive graph traversals.
- **Zero-Copy Memory Mapping:** Utilizes `mmap` with shared mapping policies to handle massive routing tables without CPU bottlenecks.
- **Fault Tolerant & Stress Tested:** Engineered with rigorous bounds checking, OOM error handling, and automated PyTest end-to-end verification.

---

## 🛠️ System Architecture

```
                 [ NVMe SSD / Dataset ]
                            │
                            ▼   (Zero-Copy mmap)
                    [ CPU RAM / Host ]
                            │
              ┌─────────────┴──────────────┐
              ▼                            ▼
   [ CSR Graph Topology ]        [ Huge Page SoA RIB ]
              │                            │
              └─────────────┬──────────────┘
                            ▼   (PCIe Bus Async Transfers)
                    [ Nvidia GPU VRAM ]
                            │
                            ▼
          [ CUDA Parallel Graph Traversal Kernel ]
```

---

## ⚙️ Requirements & Dependencies

- **OS:** Linux / Windows Subsystem for Linux (WSL2 Ubuntu)
- **Compiler:** NVCC (Nvidia CUDA Compiler) & G++ (Supporting C++17 or higher)
- **Testing:** Google Test (`libgtest-dev`) & Python 3 with PyTest

---

## 🚀 Quick Start & Compilation

1. **Clone the Repository & Configure Huge Pages:**

   ```bash
   sudo sysctl -w vm.nr_hugepages=1024
   ```

2. **Generate Synthetic Datasets:**

   ```bash
   python3 generate_data.py
   python3 generate_edge_cases.py
   ```

3. **Compile the CUDA Binary:**

   ```bash
   nvcc -O3 main.cu -o bgp_sim_gpu
   ```

4. **Run the Simulation:**

   ```bash
   ./bgp_sim_gpu
   ```

---

## 🧪 Testing & Verification

The project includes a multi-layered testing framework covering micro-level memory safety, domain logic, and hardware stress limits:

**C++ Memory & Unit Tests (GTest):**

```bash
g++ test_memory.cpp -lgtest -lgtest_main -pthread -o test_memory && ./test_memory
```

**End-to-End Pipeline Automation (PyTest):**

```bash
pytest test_pipeline.py -v
```

**Hardware Stress Test (VRAM Saturation & OOM Validation):**

```bash
python3 stress_test.py
```