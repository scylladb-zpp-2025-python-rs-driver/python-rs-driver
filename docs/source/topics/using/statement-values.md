# Statement values

Statement text is constant, while values may change between executions. Values can be passed separately from the statement text using bound parameters.

Values can be provided as:

- a list,
- a tuple,
- a mapping,
- or omitted when the statement does not need any values.

Bound parameters should be used instead of building queries by concatenating strings. This keeps the query easier to read and avoids problems such as invalid escaping or injection vulnerabilities.

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    (user_id, "Alice", 30),
)
```

## Passing positional values

Use positional values when the statement contains positional bind markers, written as `?`.

Each `?` in the statement is filled with the matching value from the provided sequence, in order. The sequence may be a tuple:

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    (user_id, "Alice", 30),
)
```

or a list:

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    [user_id, "Alice", 30],
)
```

Values may also be passed as a mapping, such as a dictionary. When a mapping is used, dictionary keys are matched by name. The order of keys does not matter:

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
    {
        "id": user_id,
        "name": "Alice",
        "age": 30,
    },
)

await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    {
        "age": 30,
        "name": "Alice",
        "id": user_id,
    },
)
```

For a single positional value, remember to pass a one-element tuple or list:

```python
await session.execute(
    "SELECT * FROM users WHERE id = ?",
    (user_id,),
)
```

If the statement does not contain any bind markers, values can be omitted:

```python
await session.execute("SELECT * FROM users")
```

## Prepared statements and statement objects

Prepared statements and statement objects accept values in the same way as plain query strings.

```python
prepared = await session.prepare(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
)

await session.execute(
    prepared,
    (user_id, "Alice", 30),
)
```

## Type conversion

Python values are converted to the CQL types expected by the database schema.

For the full mapping of supported CQL and Python types, see [Data Types](../data-types.md).
