import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts

    tokens = re.findall(r'[a-z0-9]+', text.lower())
    doc_len = len(tokens)

    if doc_len == 0:
        continue

    tf_counts = {}
    for token in tokens:
        tf_counts[token] = tf_counts.get(token, 0) + 1

    for term, tf in tf_counts.items():
        print(f"{term}\t{doc_id}\t{tf}\t{doc_len}\t{title}")
