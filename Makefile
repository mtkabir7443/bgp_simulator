CXX = g++
NVCC = nvcc
PYTHON ?= python3
CXXFLAGS = -O3 -march=native -pthread -Wall -Wextra -std=c++17
GPU_ARCH ?= sm_86
NVCCFLAGS = -O3 -std=c++17 -arch=$(GPU_ARCH)

BUILD_DIR := build
CPU_TARGET := $(BUILD_DIR)/bgp_simulator
GPU_TARGET := $(BUILD_DIR)/bgp_sim_gpu
TEST_TARGET := $(BUILD_DIR)/test_memory
PYTHON_INCLUDES = $(shell $(PYTHON) -m pybind11 --includes)
PYTHON_SUFFIX = $(shell $(PYTHON) -c 'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))')
PYTHON_TARGET = $(BUILD_DIR)/bgp_simulator$(PYTHON_SUFFIX)

# Make-driven scripts and tests use the extension built by this checkout.
export PYTHONPATH := $(abspath $(BUILD_DIR)):$(abspath benchmarks)$(if $(PYTHONPATH),:$(PYTHONPATH))

.PHONY: all cpu gpu python test pytest demo demo-gpu benchmark benchmark-synthetic clean help

all: cpu gpu python
cpu: $(CPU_TARGET)
gpu: $(GPU_TARGET)
python: $(PYTHON_TARGET)

$(BUILD_DIR):
	mkdir -p $@

$(CPU_TARGET): src/main.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $<

$(GPU_TARGET): src/main.cu | $(BUILD_DIR)
	$(NVCC) $(NVCCFLAGS) -o $@ $<

$(PYTHON_TARGET): src/main.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -DBUILD_PYTHON_MODULE -fPIC -shared $(PYTHON_INCLUDES) -o $@ $<

$(TEST_TARGET): tests/test_memory.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $< -lgtest -lgtest_main

# Legacy GoogleTest allocator experiments, separate from the routing suite.
test: $(TEST_TARGET)
	./$(TEST_TARGET)

pytest: all
	$(PYTHON) -m pytest -v

demo: cpu
	mkdir -p outputs/demo/cpu
	cd outputs/demo/cpu && $(abspath $(CPU_TARGET)) --relationships $(abspath tests/fixtures/rel_island.txt) --announcements $(abspath tests/fixtures/ann_island.txt)
	@echo "CPU routes: outputs/demo/cpu/ribs.csv"

demo-gpu: gpu
	mkdir -p outputs/demo/gpu
	cd outputs/demo/gpu && $(abspath $(GPU_TARGET)) --relationships $(abspath tests/fixtures/rel_island.txt) --announcements $(abspath tests/fixtures/ann_island.txt)
	@echo "GPU routes: outputs/demo/gpu/ribs.csv"

benchmark: python
	$(PYTHON) benchmarks/benchmark.py $(or $(ARGS),--routes 10 100)

benchmark-synthetic: python
	$(PYTHON) benchmarks/caida_benchmark.py $(or $(ARGS),--tier1 2 --tier2 4 --stubs 20 --prefixes 8)

# Only reproducible build products are removed; preserve datasets and run output.
clean:
	rm -f $(CPU_TARGET) $(GPU_TARGET) $(TEST_TARGET) $(BUILD_DIR)/bgp_simulator*.so $(BUILD_DIR)/*.o

help:
	@echo "Build: all (default), cpu, gpu, python"
	@echo "Verify: pytest (routing/Python suite), test (legacy memory experiments)"
	@echo "Run: demo, demo-gpu (small fixtures; outputs/demo/)"
	@echo "Measure: benchmark, benchmark-synthetic (override workload with ARGS='...')"
	@echo "Options: GPU_ARCH=sm_89, PYTHON=python3; use -B to rebuild after flag changes"
	@echo "Clean: remove build products only; preserve data/ and outputs/"
