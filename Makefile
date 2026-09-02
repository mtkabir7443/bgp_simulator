CXX = g++
NVCC = nvcc
CXXFLAGS = -O3 -march=native -pthread -Wall -Wextra -std=c++17
NVCCFLAGS = -O3 -std=c++17 -arch=sm_86

CPU_TARGET = bgp_simulator
GPU_TARGET = bgp_sim_gpu
TEST_TARGET = test_memory

PYTHON_INCLUDES = $(shell python3 -m pybind11 --includes 2>/dev/null)
PYTHON_SUFFIX = $(shell python3-config --extension-suffix 2>/dev/null || echo ".so")
PYTHON_TARGET = bgp_simulator$(PYTHON_SUFFIX)

all: $(CPU_TARGET) $(GPU_TARGET) $(PYTHON_TARGET)

$(CPU_TARGET): main.cpp
	$(CXX) $(CXXFLAGS) -o $(CPU_TARGET) main.cpp

$(GPU_TARGET): main.cu
	$(NVCC) $(NVCCFLAGS) -o $(GPU_TARGET) main.cu

$(PYTHON_TARGET): main.cpp
	$(CXX) $(CXXFLAGS) -DBUILD_PYTHON_MODULE -fPIC -shared $(PYTHON_INCLUDES) -o $(PYTHON_TARGET) main.cpp

test: $(TEST_TARGET)
	./$(TEST_TARGET)

$(TEST_TARGET): test_memory.cpp
	$(CXX) $(CXXFLAGS) -o $(TEST_TARGET) test_memory.cpp -lgtest -lgtest_main

clean:
	rm -f $(CPU_TARGET) $(GPU_TARGET) $(TEST_TARGET) bgp_simulator*.so *.o ribs.csv
