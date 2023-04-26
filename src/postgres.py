from psycopg2 import connect


class PostgresLoader:
    """
    A class that provides functionality for loading data into a PostgreSQL database.

    Attributes:
        conn: A psycopg2 connection object that is used to interact with the database.
        cursor: A psycopg2 cursor object that is used to execute SQL statements.
        autocommit: A boolean indicating whether autocommit is enabled or disabled.
    """

    def __init__(self, conn=None, autocommit=False):
        """
        Initializes a new PostgresLoader instance.

        Parameters:
            conn (optional): A psycopg2 connection object that is used to interact with the database.
            autocommit (optional): A boolean indicating whether autocommit should be enabled or disabled.
        """
        self.conn = conn or connect(
            host="your_host",
            database="your_database",
            user="your_username",
            password="your_password",
        )
        self.conn.autocommit = autocommit
        self.cursor = self.conn.cursor()

    def execute_query(self, query, params=None):
        """
        Executes the given SQL query with optional parameters.

        Parameters:
            query: The SQL query to execute.
            params (optional): A tuple or list of parameters to substitute into the query.
        """
        self.cursor.execute(query, params)

    def commit(self):
        """
        Commits the current transaction to the database.
        """
        self.conn.commit()

    def rollback(self):
        """
        Rolls back the current transaction to the last commit.
        """
        self.conn.rollback()

    def load_data_old(self, data, table_name):
        """
        Loads the given data into a temporary table in the PostgreSQL database.

        Parameters:
            data: A list of dictionaries representing the data to load into the database.
            table_name: The name of the temporary table to create and load the data into.
        """
        # Create a temporary table
        self.execute_query(
            f"CREATE TEMP TABLE {table_name} AS SELECT * FROM iowa_liquor_sales LIMIT 0"
        )

        # Copy data into temporary table
        columns = list(data[0].keys())
        columns_str = ",".join(columns)
        values = [tuple(d[column] for column in columns) for d in data]
        # placeholders = ",".join(["%s"] * len(columns))
        self.execute_query(
            f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV", None
        )
        self.cursor.copy_from(iter(values), null="")

    def load_data(self, data, table_name):
        """
        Load data into a temporary table in the PostgreSQL database.

        :param data: List of dictionaries containing the data to be loaded.
        :param table_name: Name of the temporary table to be created.
        """
        # Get the names of the columns from the JSON data
        columns = []
        for row in data:
            for key in row.keys():
                if key not in columns:
                    columns.append(key)

        # Create the temporary table with the appropriate columns
        create_table_query = f"CREATE TEMP TABLE {table_name} ({','.join([f'{col} TEXT' for col in columns])})"
        self.execute_query(create_table_query)

        # Load the data into the temporary table
        for row in data:
            values = [row.get(col, "") for col in columns]
            insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
            self.execute_query(insert_query, values)

        print(f"Data loaded into table {table_name}")
