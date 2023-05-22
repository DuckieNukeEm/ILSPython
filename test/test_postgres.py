import pytest
from psycopg2 import connect
from dotenv import load_dotenv
from os import getenv
from ILSPython import PostgresLoader

load_dotenv()

DBPW = getenv("DB_PW")
DBUSER = getenv("DB_USER")
DBNAME = getenv("DB_NAME")
DBHOST = getenv("DB_HOST")
conn = connect(dbname=DBNAME, user=DBUSER, password=DBPW, host=DBHOST)

data = [
    {"name": "John", "age": 30},
    {"name": "Jane", "age": 25},
    {"name": "Bob", "age": 40},
]


def test_init():
    pg = PostgresLoader(conn)
    assert pg
    assert pg.status() == 1


def test_execute_query():
    pg = PostgresLoader(conn)
    pg.execute_query("SELECT 1")
    assert pg.status() == 2
    assert pg.fetch_data() == [(1,)]
    pg.commit()


def test_run_query():
    pg = PostgresLoader(conn)
    assert pg.run_query("SELECT 1") == [(1,)]
    pg.commit()


def test_load_data():
    pg = PostgresLoader(conn)
    # Test the load_data method

    pg.load_data(data, "test_table")
    pg.execute_query("SELECT * FROM test_table")
    assert pg.fetch_data(all=True) == [
        ("John", "30"),
        ("Jane", "25"),
        ("Bob", "40"),
    ]
    pg.commit()


def test_query_execution_with_no_returns():
    pg = PostgresLoader(conn)
    # Test the run_query method
    pg.execute_query("CREATE TEMP TABLE test_query_table AS SELECT * FROM test_table")
    assert pg.run_query("SELECT * FROM test_query_table") == [
        ("John", "30"),
        ("Jane", "25"),
        ("Bob", "40"),
    ]
    pg.commit()


def test_autocommit():
    pg = PostgresLoader(conn)
    # Test the run_query method
    pg.execute_query("drop table if exists test_query_table;")
    pg.execute_query("CREATE TEMP TABLE test_query_table AS SELECT * FROM test_table")
    assert pg.run_query("SELECT * FROM test_query_table") == [
        ("John", "30"),
        ("Jane", "25"),
        ("Bob", "40"),
    ]
    pg.commit()
    # Test the commit method
    pg.set_autocommit(False)
    pg.execute_query(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)", ("Alice", "35")
    )
    assert pg.run_query("SELECT * FROM test_table WHERE name='Alice'") == [
        ("Alice", "35")
    ]
    pg.commit()


def test_rollback():
    pg = PostgresLoader(conn, autocommit=True)
    pg.begin()
    # Test the rollback method
    pg.execute_query(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)", ("Mike", "45")
    )
    pg.rollback()
    assert pg.run_query("SELECT * FROM test_table WHERE name='Mike'") == []
