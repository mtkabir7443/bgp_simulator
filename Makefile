# High-Performance BGP Simulator Makefile
# Author: Mohammed Kabir

CXX = g++
# -march=native is required for AVX2 intrinsics used in the Ludicrous Edition
# -pthread is required for the threading pool
CXXFLAGS = -O3 -march=native -pthread -Wall -Wextra

TARGET = bgp_simulator
SRC = main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET) *.o
