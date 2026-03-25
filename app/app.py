import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement


CASSANDRA_HOST = 'cassandra-server'
KEYSPACE = 'search_engine'

BATCH_SIZE = 50


def wait_for_cassandra(max_retries=30, delay=5):
    for attempt in range(max_retries):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            print(f"Connected to Cassandra on attempt {attempt + 1}")
            return cluster, session
        except Exception as e:
            print(f"Cassandra not ready (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(delay)
    print("ERROR: Could not connect to Cassandra")
    sys.exit(1)


def setup_schema(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """ % KEYSPACE)

    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            title text,
            doc_len int
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS global_stats (
            id int PRIMARY KEY,
            total_docs int,
            avg_doc_len double
        )
    """)

    print("Cassandra schema created")


def load_index_data(session):
    vocab_insert = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    postings_insert = session.prepare(
        "INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)"
    )
    doc_stats_insert = session.prepare(
        "INSERT INTO doc_stats (doc_id, title, doc_len) VALUES (?, ?, ?)"
    )

    vocab_seen = set()
    docs_seen = {}
    posting_count = 0
    batch = BatchStatement()
    batch_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 6:
            continue

        term, df_str, doc_id, tf_str, doc_len_str, title = parts

        try:
            df = int(df_str)
            tf = int(tf_str)
            doc_len = int(doc_len_str)
        except ValueError:
            continue

        if term not in vocab_seen:
            batch.add(vocab_insert, (term, df))
            batch_count += 1
            vocab_seen.add(term)

        batch.add(postings_insert, (term, doc_id, tf))
        batch_count += 1
        posting_count += 1

        if doc_id not in docs_seen:
            batch.add(doc_stats_insert, (doc_id, title, doc_len))
            batch_count += 1
            docs_seen[doc_id] = doc_len

        if batch_count >= BATCH_SIZE:
            session.execute(batch)
            batch = BatchStatement()
            batch_count = 0

    if batch_count > 0:
        session.execute(batch)

    total_docs = len(docs_seen)
    avg_doc_len = sum(docs_seen.values()) / total_docs if total_docs > 0 else 0.0

    session.execute(
        SimpleStatement(
            "INSERT INTO global_stats (id, total_docs, avg_doc_len) VALUES (1, %s, %s)"
        ),
        (total_docs, avg_doc_len)
    )

    print(f"Loaded {len(vocab_seen)} vocabulary terms")
    print(f"Loaded {posting_count} postings")
    print(f"Loaded {total_docs} document stats")
    print(f"Average document length: {avg_doc_len:.2f}")


def main():
    cluster, session = wait_for_cassandra()
    setup_schema(session)
    load_index_data(session)
    cluster.shutdown()
    print("Index data stored in Cassandra successfully")


if __name__ == "__main__":
    main()
