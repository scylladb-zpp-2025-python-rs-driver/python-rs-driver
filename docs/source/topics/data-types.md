# Data Types

The driver maps CQL data types to matching Python objects for sending values to the database and receiving values from query results.

See [Statement values](using/statement-values.md) for more information about passing values to statements. 
See [Query result](query-results.md) for more information about reading values from query results.

## Values returned by the driver

The table below shows the default Python objects returned by the driver when reading CQL values from query results.

| CQL type | Python representation |
|---|---|
| `boolean` | `bool` |
| `tinyint` | `int` |
| `smallint` | `int` |
| `int` | `int` |
| `bigint` | `int` |
| `counter` | `int` |
| `varint` | `int` |
| `float` | `float` |
| `double` | `float` |
| `ascii` | `str` |
| `text` | `str` |
| `varchar` | `str` |
| `blob` | `bytes` |
| `inet` | `ipaddress.IPv4Address` or `ipaddress.IPv6Address` |
| `uuid` | `uuid.UUID` |
| `timeuuid` | `uuid.UUID` |
| `date` | `datetime.date` |
| `time` | `datetime.time` |
| `timestamp` | `datetime.datetime` in UTC |
| `duration` | `dateutil.relativedelta.relativedelta` |
| `decimal` | `decimal.Decimal` |
| `list<T>` | `list` |
| `set<T>` | `set` |
| `map<K, V>` | `dict` |
| `tuple<...>` | `tuple` |
| `udt` | `dict[str, value]` |
| `vector<T>` | `list` |
| `null` | `None` |

## Values accepted by the driver

The table below shows the Python objects that can currently be provided when sending values to the database.

| CQL type | Accepted Python representation |
|---|---|
| `boolean` | `bool` |
| `tinyint` | `int` |
| `smallint` | `int` |
| `int` | `int` |
| `bigint` | `int` |
| `counter` | `int` |
| `varint` | `int` |
| `float` | `float` |
| `double` | `float` |
| `ascii` | `str` |
| `text` | `str` |
| `varchar` | `str` |
| `blob` | `bytes` |
| `inet` | `ipaddress.IPv4Address` or `ipaddress.IPv6Address` |
| `uuid` | `uuid.UUID` |
| `timeuuid` | `uuid.UUID` |
| `date` | `datetime.date` |
| `time` | `datetime.time` |
| `timestamp` | `datetime.datetime` |
| `duration` | `dateutil.relativedelta.relativedelta` |
| `decimal` | `decimal.Decimal` |
| `list<T>` | `list` |
| `set<T>` | `set` |
| `map<K, V>` | `dict` |
| `tuple<...>` | `tuple` |
| `udt` | `dict[str, value]` |
| `vector<T>` | `list` |
| `null` | `None` |

Values supplied by the user are validated against the CQL types expected by the database schema. If a value does not match the expected type, the driver returns an error.
The set of accepted input types may be extended in the future. For now, the accepted input types match the default types returned by the driver.
