# Ignore redefined-while-unused (F811) & unused-import (F401)
# as test is is about checking that various ways of importing stuff work
# ruff: noqa: F811 F401
def test_imports():
    import scylla as X  # pyright: ignore[reportUnusedImport]
    import scylla.session as X  # pyright: ignore[reportUnusedImport]
    import scylla.session_builder as X  # pyright: ignore[reportUnusedImport]
    from scylla import session as X  # pyright: ignore[reportUnusedImport]
    from scylla import session_builder as X  # pyright: ignore[reportUnusedImport]
    from scylla.session import Session as X  # pyright: ignore[reportUnusedImport]
    from scylla.session_builder import SessionBuilder as X  # pyright: ignore[reportUnusedImport]


def test_nested_module_imports():
    import scylla.cluster.metadata as X  # pyright: ignore[reportUnusedImport]
    from scylla.cluster.metadata import CqlDate, CqlTinyInt  # pyright: ignore[reportUnusedImport]
    from scylla.cluster.metadata import Keyspace as X  # pyright: ignore[reportUnusedImport]
