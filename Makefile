# Makefile for RocksDB Benchmark
#
# Usage:
#   make              - Build the benchmark
#   make clean        - Clean build files
#   make run          - Build and run the benchmark

CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra
LDFLAGS = -lrocksdb -lpthread -ldl -lz -lbz2 -lsnappy -llz4 -lzstd

# RocksDB include path (adjust if needed)
ROCKSDB_INCLUDE = /usr/include
ROCKSDB_LIB = /usr/lib

# Targets
TARGET = rocksdb_benchmark
SOURCES = rocksdb_benchmark.cpp
OBJECTS = $(SOURCES:.cpp=.o)

# Default target
all: $(TARGET)

# Build the benchmark
$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -L$(ROCKSDB_LIB) $(LDFLAGS)

# Compile source files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -I$(ROCKSDB_INCLUDE) -c $< -o $@

# Run the benchmark
run: $(TARGET)
	./$(TARGET)

# Clean build artifacts
clean:
	rm -f $(TARGET) $(OBJECTS)
	rm -rf benchmark_rocksdb benchmark_bulk_rocksdb

# Clean database files
cleandb:
	rm -rf benchmark_rocksdb benchmark_bulk_rocksdb

# Full clean
distclean: clean cleandb

.PHONY: all run clean cleandb distclean
