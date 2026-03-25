set -e

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

DATA_DIR="data"
HDFS_DATA="/data"
HDFS_INPUT="/input/data"

if [ -f "a.parquet" ]; then
    echo "Found a.parquet, extracting documents locally..."
    MAX_DOCS="${MAX_DOCS:-}"
    if [ -n "$MAX_DOCS" ]; then
        spark-submit prepare_data.py extract "$MAX_DOCS"
    else
        spark-submit prepare_data.py extract
    fi
    echo "Documents extracted from parquet"
else
    echo "No a.parquet found, using existing documents in $DATA_DIR/"
fi

DOC_COUNT=$(ls $DATA_DIR/*.txt 2>/dev/null | wc -l)
echo "Found $DOC_COUNT documents in $DATA_DIR/"

if [ "$DOC_COUNT" -eq 0 ]; then
    echo "ERROR: No documents found in $DATA_DIR/. Please provide a.parquet or pre-extracted documents."
    exit 1
fi

echo "Uploading documents to HDFS $HDFS_DATA..."
hdfs dfs -rm -r -f $HDFS_DATA
hdfs dfs -mkdir -p $HDFS_DATA
hdfs dfs -put $DATA_DIR/*.txt $HDFS_DATA/
echo "Documents uploaded to HDFS"
hdfs dfs -ls $HDFS_DATA | head -20

echo "Creating input dataset at $HDFS_INPUT..."
hdfs dfs -rm -r -f $HDFS_INPUT
spark-submit prepare_data.py prepare
echo "Input dataset created"
hdfs dfs -ls $HDFS_INPUT

echo "Done data preparation!"
