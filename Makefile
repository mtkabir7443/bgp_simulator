CXX = g++
NVCC = nvcc
CXXFLAGS = -O3 -march=native -pthread -Wall -Wextra -std=c++17

# Compute capability of the target GPU.
#   sm_86 = Ampere  (RTX 30-series, A-series)
#   sm_89 = Ada     (RTX 40-series)
#   sm_90 = Hopper  (H100)
# Override without editing this file:  make GPU_ARCH=sm_89
GPU_ARCH ?= sm_86
NVCCFLAGS = -O3 -std=c++17 -arch=$(GPU_ARCH)

CPU_TARGET = bgp_simulator
GPU_TARGET = bgp_sim_gpu
TEST_TARGET = test_memory

PYTHON_INCLUDES = $(shell python3 -m pybind11 --includes 2>/dev/null)
PYTHON_SUFFIX = $(shell python3-config --extension-suffix 2>/dev/null || echo ".so")
PYTHON_TARGET = bgp_simulator$(PYTHON_SUFFIX)

.PHONY: all cpu gpu python test pytest clean help

all: $(CPU_TARGET) $(GPU_TARGET) $(PYTHON_TARGET)

cpu: $(CPU_TARGET)
gpu: $(GPU_TARGET)
python: $(PYTHON_TARGET)

$(CPU_TARGET): main.cpp
	$(CXX) $(CXXFLAGS) -o $(CPU_TARGET) main.cpp

$(GPU_TARGET): main.cu
	$(NVCC) $(NVCCFLAGS) -o $(GPU_TARGET) main.cu

$(PYTHON_TARGET): main.cpp
	$(CXX) $(CXXFLAGS) -DBUILD_PYTHON_MODULE -fPIC -shared $(PYTHON_INCLUDES) -o $(PYTHON_TARGET) main.cpp

$(TEST_TARGET): test_memory.cpp
	$(CXX) $(CXXFLAGS) -o $(TEST_TARGET) test_memory.cpp -lgtest -lgtest_main

# GoogleTest allocator suite
test: $(TEST_TARGET)
	./$(TEST_TARGET)

# Full Python-side suite; depends on all three build products
pytest: all
	pytest test_pipeline.py -v
	python3 test_python_binding.py

clean:
	rm -f $(CPU_TARGET) $(GPU_TARGET) $(TEST_TARGET) bgp_sim bgp_simulator*.so *.o ribs.csv

help:
	@echo "Targets:"
	@echo "  all      Build CPU binary, GPU binary, and Python extension"
	@echo "  cpu      Build $(CPU_TARGET) only"
	@echo "  gpu      Build $(GPU_TARGET) only (override arch: make gpu GPU_ARCH=sm_89)"
	@echo "  python   Build the PyBind11 extension module only"
	@echo "  test     Build and run the GoogleTest allocator suite"
	@echo "  pytest   Build everything, then run the Python test suite"
	@echo "  clean    Remove all build artifacts and ribs.csv"
