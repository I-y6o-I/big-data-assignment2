import sys
import re
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import pyarrow.fs as pafs



def normalize_title(title):
    clean = re.sub(r"\s+", " ", str(title or "")).strip()
    clean = clean.replace(" ", "_")
    clean = sanitize_filename(clean)
    return clean or "untitled"


def clean_wiki_text(text):
    clean = str(text or "")
    clean = re.sub(r"<ref[^>]*>.*?</ref>", " ", clean, flags=re.IGNORECASE | re.DOTALL)
    clean = re.sub(r"\{\{[^{}]*\}\}", " ", clean)
    clean = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]+)\]\]", r"\1", clean)
    clean = re.sub(r"\[(?:https?://|ftp://)[^\s\]]+(?:\s+([^\]]+))?\]", r"\1", clean)
    clean = re.sub(r"'{2,}", "", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def parse_document(pair):
    filepath, content = pair
    filename = filepath.split("/")[-1]
    name = filename.replace(".txt", "")
    idx = name.find("_")
    if idx == -1:
        doc_id = name
        title = name
    else:
        doc_id = name[:idx]
        title = name[idx + 1:]
    text = content.replace("\t", " ").replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return doc_id + "\t" + title + "\t" + text


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.sql.parquet.columnarReaderBatchSize", 32) \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

HDFS_URI = "hdfs://cluster-master:9000"

fs_driver, _ = pafs.FileSystem.from_uri(HDFS_URI)
fs_driver.create_dir("/data", recursive=True)

def create_doc(row):
    fs, _ = pafs.FileSystem.from_uri(HDFS_URI)

    doc_id = str(row["id"] or "").strip()
    if not doc_id:
        return

    title = normalize_title(row["title"])
    text = clean_wiki_text(row["text"])
    if not text:
        return

    hdfs_path = f"/data/{doc_id}_{title}.txt"
    with fs.open_output_stream(hdfs_path) as out:
        out.write(text.encode("utf-8"))


df.foreach(create_doc)


print("====== Uploading docs to hdfs://input/data ======")
hdfs_data_path = "hdfs:///data"
hdfs_output_path = "hdfs:///input/data"

if sc is None:
    raise RuntimeError("Spark context is not initialized")

rdd = sc.wholeTextFiles(hdfs_data_path)

output_rdd = rdd.map(parse_document).filter(lambda line: len(line.split("\t", 2)) == 3 and line.split("\t", 2)[2].strip() != "").coalesce(1)
output_rdd.saveAsTextFile(hdfs_output_path)
print("Created /input/data in HDFS")

if spark is not None:
    spark.stop()
