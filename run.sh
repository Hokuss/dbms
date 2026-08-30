#!/bin/bash

# Exit immediately if any command fails (equivalent to checking %ERRORLEVEL%)
set -e

# 1. Configure
cmake -B build

# 2. Build
cmake --build build

# 3. Run
echo ""
echo "=== Running Executable ==="
./build/sandbox