set -e

INPUT_PATH="${1:-/input/data}"
OUTPUT_PATH="/indexer/index"

echo "=== Creating index using MapReduce ==="
echo "Input path: $INPUT_PATH"
echo "Output path: $OUTPUT_PATH"

hdfs dfs -rm -r -f $OUTPUT_PATH

STREAMING_JAR=$(find $HADOOP_HOME/share/hadoop/tools/lib/ -name "hadoop-streaming-*.jar" | head -1)

if [ -z "$STREAMING_JAR" ]; then
    echo "ERROR: hadoop-streaming jar not found"
    exit 1
fi

echo "Using streaming jar: $STREAMING_JAR"

hadoop jar $STREAMING_JAR \
    -input $INPUT_PATH \
    -output $OUTPUT_PATH \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

echo "MapReduce job completed"
hdfs dfs -ls $OUTPUT_PATH
echo "=== Index creation done ==="
