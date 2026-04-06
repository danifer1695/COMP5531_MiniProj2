CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -I include

SRC_DIR = src
INC_DIR = include
BUILD_DIR = build

# Source files (add more as the project grows)
SOURCES = $(SRC_DIR)/DiskManager.cpp $(SRC_DIR)/Record.cpp

# Default target: build and run tests
all: test_basic

# Build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Test binary
#test_basic: $(BUILD_DIR) $(SOURCES) $(SRC_DIR)/test_basic.cpp
	#$(CXX) $(CXXFLAGS) -o $(BUILD_DIR)/test_basic $(SOURCES) $(SRC_DIR)/test_basic.cpp

# Run tests
test: test_basic
	./$(BUILD_DIR)/test_basic

# Clean
clean:
	rm -rf $(BUILD_DIR) tmp/*.dat output/*

.PHONY: all test clean