from scylla.session_builder import SessionBuilder;

def test_cluster_connect():
    builder = SessionBuilder(['172.42.0.2'], 9042)
    session = builder.connect()

def test_simple_query():
    builder = SessionBuilder(['172.42.0.2'], 9042)
    session = builder.connect()
    result = session.execute("SELECT * FROM system.local")
    print(result)
