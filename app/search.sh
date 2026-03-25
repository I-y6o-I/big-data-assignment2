set -e

if [ -z "$1" ]; then
    echo "Usage: bash search.sh \"your search query\""
    exit 1
fi

QUERY="$1"
echo "=== Searching for: \"$QUERY\" ==="

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    query.py "$QUERY"

echo "=== Search complete ==="
