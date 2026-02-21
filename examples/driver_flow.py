import asyncio

from scylla.enums import Consistency
from scylla.execution_profile import ExecutionProfile
from scylla.session_builder import SessionBuilder


async def main():
    # Lets connect to cluster and establish a session
    session = await SessionBuilder(["127.0.0.2"], 9042).connect()

    # Let's create a table
    table_name = "example_table"
    schema = "id int PRIMARY KEY, value text"
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS example_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    )
    await session.execute("USE example_ks;")
    await session.execute(f"DROP TABLE IF EXISTS {table_name};")
    await session.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")

    # Now populate it

    list_of_values = [[67, "My favorite number"], [0, "First natural number"], [3, "Almost pi"]]

    for values in list_of_values:
        await session.execute(f"INSERT INTO {table_name} (id, value) VALUES (?, ?)", values)

    # Let's see how the table now looks
    result = await session.execute(f"SELECT * FROM {table_name}")
    for row in result.iter_rows():
        print(f"id: {row.get('id')}")
        print(f"value: {row.get('value')}")
        print(f"Rows are deserialized as dicts. Whole row:\n {row}\n")

    # That worked but I want to insert a lot of rows I better prepare the statement
    prepared_statement = await session.prepare(f"INSERT INTO {table_name} (id, value) VALUES (?, ?)")

    # Also let's:
    # - increase request timeout for query
    # - demand consistency ALL so state is consistant after insertion

    prepared_statement = prepared_statement.with_request_timeout(10.0).with_consistency(Consistency.All)

    # Now we are ready for inserting rows
    for i in range(10):
        await session.execute(prepared_statement, [i, f"This is number {i}"])

    # Let's check our table
    result = await session.execute(f"SELECT * FROM {table_name}")
    for row in result.iter_rows():
        print(row)

    # Now let's create a more complex table
    complex_table_name = "complex_table"
    complex_schema = "student_id int PRIMARY KEY, subject_marks map<text, int>"
    await session.execute(f"DROP TABLE IF EXISTS {complex_table_name};")
    await session.execute(f"CREATE TABLE IF NOT EXISTS {complex_table_name} ({complex_schema});")

    # Prepare statement and create an Execution Profile with all our requirements
    execution_profile = ExecutionProfile(timeout=3.0, consistency=Consistency.One)
    complex_prepared = await session.prepare(
        f"INSERT INTO {complex_table_name} (student_id, subject_marks) VALUES (?, ?)"
    )
    complex_prepared = complex_prepared.with_execution_profile(execution_profile)

    # Insert some rows
    for id in range(6):
        totally_real_marks = {"Math": (id + 1) % 5 + 1, "Science": (id + 2) % 5 + 1, "English": (id + 3) % 5 + 1}
        await session.execute(complex_prepared, [id, totally_real_marks])

    # See the results
    result = await session.execute(f"SELECT * FROM {complex_table_name}")
    for row in result.iter_rows():
        print(row)


asyncio.run(main())
