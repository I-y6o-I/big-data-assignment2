service ssh restart

echo "=== Starting Hadoop services ==="
bash start-services.sh

echo "=== Setting up Python environment ==="
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -o .venv.tar.gz

echo "=== Uploading parquet file ==="
hdfs dfs -rm -f /a.parquet
hdfs dfs -put -f /app/a.parquet /a.parquet
hdfs dfs -ls /a.parquet

echo "=== Preparing data ==="
export CLASSPATH="$(hadoop classpath --glob)"
export ARROW_LIBHDFS_DIR=/usr/local/hadoop/lib/native
bash prepare_data.sh

echo "=== Data check ==="
hdfs dfs -ls /data | head

echo "=== Data check ==="
hdfs dfs -count /data

echo "=== Data check ==="
hdfs dfs -ls /input/data


echo "=== Running indexer ==="
bash index.sh

echo "=== Running sample searches ==="
bash search.sh "world war history"
bash search.sh "music album rock"
bash search.sh "united states president"

echo "=== All done ==="
