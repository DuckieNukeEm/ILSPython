from psycopg2 import connect, extensions


class PostgresLoader:
    """
    A class that provides functionality for loading data into a PostgreSQL database.

    Attributes:
        conn: A psycopg2 connection object that is used to interact with the database.
        cursor: A psycopg2 cursor object that is used to execute SQL statements.
        autocommit: A boolean indicating whether autocommit is enabled or disabled.
    """

    def __init__(self, conn=None, conn_details: dict = None, autocommit: bool = False):
        """
        Initializes a new PostgresLoader instance.

        Parameters:
            conn (optional): A psycopg2 connection object that is used to interact with the database.
            autocommit (optional): A boolean indicating whether autocommit should be enabled or disabled.
        """
        self.conn = conn or connect(**conn_details)
        self.set_autocommit(autocommit)
        self.cursor = self.conn.cursor()

    def set_autocommit(self, autocommit: bool = True):
        """sets the autocommit status

        Parameters:
            autocommit (bool, option): the value of autocommit to set
        """
        if self.conn.autocommit != autocommit:
            self.conn.autocommit = autocommit

    def execute_query(self, query, params=None):
        """
        Executes the given SQL query with optional parameters.

        Parameters:
            query: The SQL query to execute.
            params (optional): A tuple or list of parameters to substitute into the query.


        Details:
            This differs from run_query in that it doesn't return the results.
            Results have to be retrieved via fetch_data.
        """
        self.cursor.execute(query, params)

    def run_query(self, query, params=None):
        """Executes the given SQL query with optional parameters and returns the resuls

        Parameters:
            query: The SQL query to execute.
            params (optional): A tuple or list of parameters to substitute into the query.

        Details:
            This differs from execute_query in that it actually returns the results of the query.
        """
        self.cursor.execute(query, params)
        return self.fetch_data()

    def fetch_data(self, all: bool = True) -> list:
        """Returns the results from an execute_query statement


        Args:
            all (bool, optional): return all the results at one (True)
                                  or just one records (False).
                                  Defaults to True.

        Returns:
            list of tuples
        """
        if all:
            return self.cursor.fetchall()
        else:
            return self.cursor.fetchone()

    def status(self, string: bool = True) -> str:
        """returns the status of the ocnnection

        Args:
            string (bool, optional): return a descriptor of the status.
                                      Defaults to True.

        Returns:
            str: a discriptor of the status (if string is True)
            or
            int: a interger value of the status (if string is False)
        """
        if self.conn.status == extensions.STATUS_READY and string:
            print("status: Connection is ready for a transaction.")

        elif self.conn.status == extensions.STATUS_BEGIN and string:
            print("psycopg2 status #2: An open transaction is in process.")

        elif self.conn.status == extensions.STATUS_IN_TRANSACTION and string:
            print("psycopg2 status #3: An exception has occured.")
            print("Use tpc_commit() or tpc_rollback() to end transaction")

        elif self.conn.status == extensions.STATUS_PREPARED and string:
            print(
                "psycopg2 status #4:A transcation is in the 2nd phase of the process."
            )

        return self.conn.status

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

    def begin(self):
        """Puts a begin statement into the transactions"""
        self.conn.begin()

    def close(self):
        """closes the connection"""
        self.conn.close()

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
