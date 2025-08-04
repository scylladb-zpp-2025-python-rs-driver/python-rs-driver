from scylla.session_builder import SessionBuilder

builder = SessionBuilder(['172.42.0.2'], 9042)
session = builder.connect()
print(session.execute("CREATE KEYSPACE IF NOT EXISTS example_ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"))
print(session.execute("CREATE TABLE example_ks.t (a int PRIMARY KEY, b text, c int)"))
print(session.execute("INSERT INTO example_ks.t (a, b, c) VALUES (1, 'sdasdad', 3)"))
print(session.execute("SELECT a, b, c FROM example_ks.t WHERE a = 1"))
