import sys

current_term = None
postings = []

def flush_term(term, postings):
    df = len(postings)
    for doc_id, tf, doc_len, title in postings:
        print(f"{term}\t{df}\t{doc_id}\t{tf}\t{doc_len}\t{title}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 4)
    if len(parts) != 5:
        continue

    term, doc_id, tf, doc_len, title = parts

    if term != current_term:
        if current_term is not None:
            flush_term(current_term, postings)
        current_term = term
        postings = []

    postings.append((doc_id, tf, doc_len, title))

if current_term is not None:
    flush_term(current_term, postings)
