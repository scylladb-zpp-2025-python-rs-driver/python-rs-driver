# Logging

The driver uses Python's built-in
[logging module](https://docs.python.org/3/library/logging.html).


## Logging level

The following logging levels are available:
 - `CRITICAL` (50),
 - `ERROR` (40),
 - `WARNING` (30),
 - `INFO` (20),
 - `DEBUG` (10),
 - `TRACE` (5).

`CRITICAL`, `ERROR`, `WARNING`, `INFO` and `DEBUG` can be configured in two standard ways:

```python
logging.basicConfig(level=ERROR, ...)
logging.basicConfig(level=40, ...)
```

However, to use the very verbose `TRACE` level (more detailed than `DEBUG`),
you must explicitly set the level to 5:

```python
logging.basicConfig(level=5, ...)
```

## Example

```python
import logging

logger = logging.getLogger(__name__)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    host = "127.0.0.2"
    port = 9042
    logger.info(f"Connecting to {host}:{port}")

    session = await SessionBuilder().contact_points([(host, port)]).connect()

    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS example_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    )
    await session.execute("USE example_ks;")


asyncio.run(main())
```

The full [example](https://github.com/scylladb-zpp-2025-python-rs-driver/python-rs-driver/blob/main/examples/log.py)
is available in the `examples` folder. You can run it from the main folder
of the driver repository using:

```bash
uv run examples/log.py
```
