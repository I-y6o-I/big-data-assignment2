import sys
import re
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster


CASSANDRA_HOST = 'cassandra-server'
KEYSPACE = 'search_engine'

K1 = 1.2
B = 0.75


def get_cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    return cluster, session


def tokenize(text):
    return re.findall(r'[a-z0-9]+', text.lower())


def fetch_global_stats(session):
    row = session.execute("SELECT total_docs, avg_doc_len FROM global_stats WHERE id = 1").one()
    if row is None:
        print("ERROR: No global stats found in Cassandra")
        sys.exit(1)
    return row.total_docs, row.avg_doc_len


def fetch_vocabulary(session, terms):
    vocab = {}
    for term in terms:
        row = session.execute(
            "SELECT df FROM vocabulary WHERE term = %s", (term,)
        ).one()
        if row is not None:
            vocab[term] = row.df
    return vocab


def fetch_postings(session, terms):
    postings = []
    for term in terms:
        rows = session.execute(
            "SELECT doc_id, tf FROM postings WHERE term = %s", (term,)
        )
        for row in rows:
            postings.append((term, row.doc_id, row.tf))
    return postings


def fetch_doc_stats(session, doc_ids):
    doc_stats = {}
    for doc_id in doc_ids:
        row = session.execute(
            "SELECT title, doc_len FROM doc_stats WHERE doc_id = %s", (doc_id,)
        ).one()
        if row is not None:
            doc_stats[doc_id] = (row.title, row.doc_len)
    return doc_stats


def main():
    if len(sys.argv) < 2:
        print("Usage: query.py <search query>")
        sys.exit(1)

    query_text = " ".join(sys.argv[1:])
    print(f"Query: {query_text}")

    query_terms = tokenize(query_text)
    if not query_terms:
        print("No valid terms in query")
        sys.exit(0)

    query_terms = list(set(query_terms))
    print(f"Query terms: {query_terms}")

    spark = SparkSession.builder \
        .appName("search_engine_query") \
        .getOrCreate()
    sc = spark.sparkContext

    cluster, session = get_cassandra_session()

    total_docs, avg_doc_len = fetch_global_stats(session)
    print(f"Total docs: {total_docs}, Avg doc length: {avg_doc_len:.2f}")

    vocab = fetch_vocabulary(session, query_terms)
    found_terms = [t for t in query_terms if t in vocab]

    if not found_terms:
        print("None of the query terms found in the index")
        cluster.shutdown()
        spark.stop()
        sys.exit(0)

    print(f"Found terms in index: {found_terms}")

    postings = fetch_postings(session, found_terms)

    if not postings:
        print("No postings found for query terms")
        cluster.shutdown()
        spark.stop()
        sys.exit(0)

    doc_ids_in_postings = list(set(p[1] for p in postings))
    doc_stats = fetch_doc_stats(session, doc_ids_in_postings)

    cluster.shutdown()

    vocab_bc = sc.broadcast(vocab)
    doc_stats_bc = sc.broadcast(doc_stats)
    N = total_docs
    avgdl = avg_doc_len

    postings_rdd = sc.parallelize(postings)

    def compute_bm25_score(posting):
        term, doc_id, tf = posting
        v = vocab_bc.value
        ds = doc_stats_bc.value

        df = v.get(term, 0)
        if df == 0 or doc_id not in ds:
            return (doc_id, 0.0)

        doc_len = ds[doc_id][1]
        idf = math.log(N / df)
        tf_component = (tf * (K1 + 1)) / (tf + K1 * (1 - B + B * doc_len / avgdl))
        score = idf * tf_component
        return (doc_id, score)

    results = postings_rdd \
        .map(compute_bm25_score) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: -x[1]) \
        .take(10)

    print("\n" + "=" * 60)
    print(f"Top 10 results for: \"{query_text}\"")
    print("=" * 60)

    for rank, (doc_id, score) in enumerate(results, 1):
        title = doc_stats.get(doc_id, ("Unknown", 0))[0]
        print(f"{rank}. {doc_id} {title} (score: {score:.4f})")

    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
