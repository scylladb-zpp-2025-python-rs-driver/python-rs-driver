# Query Results

## Overview

`Session.execute()` returns a `RequestResult`.

For `SELECT` statements, the result contains rows. By default, paging is enabled, so rows are fetched page by page instead of loading the whole result set in one response.

For statements that do not return rows, such as `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, or `DROP TABLE`, the returned `RequestResult` can usually be ignored after the request succeeds.

## RequestResult

### Row representation

By default, each row is represented as a dictionary, where keys are column names and values are the corresponding row values:

```python
{
    "a": 1,
    "b": 2,
    "c": "abc",
}
```

The exact row representation can be changed with a custom `RowFactory`. See [Custom row shapes with RowFactory](#custom-row-shapes-with-rowfactory).

### NULL values

CQL `null` values are represented as `None` in Python.

```python
result = await session.execute("SELECT a, b, c FROM select_paging")

async for row in result:
    value = row["c"]

    if value is None:
        print("column c is NULL")
    else:
        print(value)
```

### Immutability

`RequestResult` represents one immutable query result state.

Methods that need to move forward through the result set do not mutate the existing object. For example, `fetch_next_page()` returns a new `RequestResult` containing the next page.

```python
result = await session.execute(stmt)

while True:
    for row in result.iter_current_page():
        print(row)

    next_result = await result.fetch_next_page()
    if next_result is None:
        break

    result = next_result
```

This is important because the original `RequestResult` still represents the page it was created with. If you manually fetch another page, assign the returned value to a new variable or replace the old one.

## Paging

### Why paging is enabled by default

Paging is enabled by default and should usually stay enabled for `SELECT` queries.
Issuing unpaged SELECTs may have dramatic performance consequences! BEWARE!
If the result set is big, or if there are a lot of tombstones, those atrocities can happen:

  - the cluster may experience high load,
  - queries may time out,
  - the driver may devour a lot of RAM,
  - latency will likely spike.

Stay safe. Page your SELECTs.

### Configuring page size

The page size controls how many rows the database should return in one page.

```python
stmt = Statement("SELECT a, b, c FROM select_paging").with_page_size(100)

result = await session.execute(stmt)
```

Prepared statements can also carry a page size:

```python
prepared = await session.prepare("SELECT a, b, c FROM select_paging")
prepared = prepared.with_page_size(100)

result = await session.execute(prepared)
```

A smaller page size reduces per-page memory usage but may require more round trips. A larger page size reduces the number of round trips but increases the amount of data held in memory per page.

### Unpaged queries

If you explicitly want to execute an unpaged query, pass `paged=False`:

```python
result = await session.execute(
    "SELECT a, b, c FROM my_table",
    paged=False,
)
```

## Consuming results

### Async iteration

The recommended way to consume rows is to iterate over the `RequestResult`.

```python
result = await session.execute("SELECT a, b, c FROM select_paging")

async for row in result:
    print(row)
```

Async iteration automatically fetches the next page when the current page is exhausted.

### Consuming only the current page

Use `iter_current_page()` when you want to process only the rows that are already available in the current page.

```python
stmt = Statement("SELECT a, b, c FROM select_paging").with_page_size(6)
result = await session.execute(stmt)

page_rows = list(result.iter_current_page())
print(page_rows)
```

`iter_current_page()` does not fetch more data from the database. It only iterates over the current page.

To check whether another page is available, use `has_more_pages()`:

```python
if result.has_more_pages():
    next_result = await result.fetch_next_page()
```

`fetch_next_page()` returns:

- a new `RequestResult` when another page exists,
- `None` when there are no more pages.

### Manual paging

Manual paging is useful when you want explicit control over page boundaries.

```python
stmt = Statement("SELECT a, b, c FROM select_paging").with_page_size(6)
result = await session.execute(stmt)

page_no = 1

while True:
    page = list(result.iter_current_page())
    print(f"page {page_no}: {page}")

    if not result.has_more_pages():
        break

    result = await result.fetch_next_page()

    page_no += 1
```

This pattern keeps only one page in memory at a time.

### Resuming with PagingState

A `PagingState` represents the position after the current page. It can be used to resume a paged query later.

```python
prepared = await session.prepare("SELECT a, b, c FROM select_paging")
prepared = prepared.with_page_size(5)

result = await session.execute(prepared)

while True:
    page = list(result.iter_current_page())
    print(page)

    state = result.paging_state()
    if state is None:
        break

    result = await session.execute(
        prepared,
        paging_state=state,
    )
```

When `paging_state()` returns `None`, there are no more pages.

A paging state can also be serialized if it needs to be stored and used later:

```python
state = result.paging_state()

if state is None:
    return

raw_state = state.as_bytes()

# Some time passes here. The application may store raw_state
# and later use it to resume the query.
...

restored_state = PagingState.from_bytes(raw_state)
```

### Convenience helpers

`RequestResult` also provides convenience helpers for common cases.

#### `first_row()`

`first_row()` returns the first available row or `None` if the result set is empty.

```python
result = await session.execute("SELECT a, b, c FROM select_paging")

row = await result.first_row()
if row is not None:
    print(row)
```

`first_row()` may fetch additional pages if the current page does not contain a row. It does not mutate the `RequestResult`.

#### `all()`

`all()` returns all remaining rows as a list.

```python
result = await session.execute("SELECT a, b, c FROM select_paging")

rows = await result.all()
```

It eagerly fetches all remaining pages and materializes all rows in memory.

## Custom row shapes with RowFactory

The exact row representation can be changed with a custom `RowFactory`.

A row factory receives a `ColumnIterator` and builds the row object.

```python
from typing import Any, Dict

from scylla.results import RowFactory, ColumnIterator


class UppercaseKeysDictFactory(RowFactory):
    def build(self, column_iterator: ColumnIterator) -> Dict[str, Any]:
        return {
            column.column_name.upper(): column.value
            for column in column_iterator
        }
```

Use the factory when executing the query:

```python
stmt = Statement("SELECT a, b, c FROM select_paging")

result = await session.execute(
    stmt,
    factory=UppercaseKeysDictFactory(),
)

async for row in result:
    print(row)
```

This can be used to return dictionaries, dataclasses, selected columns, or any other application-specific row representation.


## Choosing the right result-consumption method

| Use case | Recommended API |
|---|---|
| Process all rows safely | `async for row in result` |
| Process one page at a time | `iter_current_page()` + `fetch_next_page()` |
| Resume later from a known position | `paging_state()` + `execute(..., paging_state=state)` |
| Get one optional row | `await result.first_row()` |
| Materialize a result set | `await result.all()` |
| Customize row representation | `factory=...` |

## Best practices

- Keep paging enabled for `SELECT` queries.
- Avoid unpaged queries for large or unbounded result sets.
- Prefer async iteration for simple row processing.
- Use manual paging when page boundaries matter.
- Remember that `RequestResult` is immutable; fetching the next page returns a new `RequestResult`.
- Use prepared statements for paging queries.
- Choose a page size based on memory usage, latency, and expected result size.
