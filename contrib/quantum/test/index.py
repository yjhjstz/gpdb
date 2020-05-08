import postgresql


def setup_db():
    db = postgresql.open('pq://luban@127.0.0.1/pg')
    db.execute("create extension if not exists quantum;")
    db.execute("set quantum.index_parallel TO 10;")
    db.execute("create index sift_idx on tt using quantum_hnsw (id, c1 ) WITH (m=20, efbuild=128, dims=128, efsearch=32);")

setup_db()