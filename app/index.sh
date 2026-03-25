#!/bin/bash
set -e

INPUT_PATH="${1:-/input/data}"

echo "=== Full indexing pipeline ==="

echo "Step 1: Creating index with MapReduce..."
bash create_index.sh "$INPUT_PATH"

echo "Step 2: Storing index in Cassandra..."
bash store_index.sh

echo "=== Full indexing pipeline completed ==="
