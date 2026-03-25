#!/bin/bash
set -e

source .venv/bin/activate

INDEX_PATH="/indexer/index"

echo "=== Storing index data to Cassandra ==="
echo "Reading index from HDFS: $INDEX_PATH"

hdfs dfs -ls $INDEX_PATH

echo "Loading data into Cassandra..."
hdfs dfs -cat $INDEX_PATH/part-* | python3 app.py

echo "=== Index stored in Cassandra ==="
