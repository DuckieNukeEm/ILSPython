from psycopg2 import connect
from ILSPython.postgres import Postgres


def test_postgres():
    # Connect to the PostgreSQL database
    conn = connect(
        dbname="test_db", user="test_user", password="test_password", host="localhost"
    )

    # Create a new instance of the Postgres class
    pg = Postgres(conn)

    # Test the execute_query method
    assert pg.execute_query("SELECT 1") == [(1,)]

    # Test the load_data method
    data = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25},
        {"name": "Bob", "age": 40},
    ]
    pg.load_data(data, "test_table")

    # Test that the data was loaded correctly
    assert pg.execute_query("SELECT * FROM test_table") == [
        ("John", "30"),
        ("Jane", "25"),
        ("Bob", "40"),
    ]

    # Test the run_query method
    pg.run_query("CREATE TEMP TABLE test_query_table AS SELECT * FROM test_table")
    assert pg.execute_query("SELECT * FROM test_query_table") == [
        ("John", "30"),
        ("Jane", "25"),
        ("Bob", "40"),
    ]

    # Test the commit method
    pg.begin()
    pg.execute_query(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)", ("Alice", "35")
    )
    pg.commit()
    assert pg.execute_query("SELECT * FROM test_table WHERE name='Alice'") == [
        ("Alice", "35")
    ]

    # Test the rollback method
    pg.begin()
    pg.execute_query(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)", ("Mike", "45")
    )
    pg.rollback()
    assert pg.execute_query("SELECT * FROM test_table WHERE name='Mike'") == []
