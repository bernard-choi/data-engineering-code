"""
@author: yunsik choi
"""

from sqlalchemy import create_engine, text
import pandas as pd


class Database:
    def __init__(self, host: str, user: str, password: str, db: str, port: str):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port

    def connect(self):
        connect_args = {"ssl": {"fake_flag_to_enable_tls": True}}
        db = "{dbtype}://{user}:{password}@{host}:{port}/{db}?charset=utf8&ssl=true".format(
            dbtype="mysql+pymysql",
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            db=self.db,
        )
        engine = create_engine(db, connect_args=connect_args, pool_recycle=3600)
        conn = engine.connect()
        return conn

    def conn_execute_query(self, query: str) -> str:
        """Execute query with db connection.

        Args:
            query: A query string that I want to execute.

        Returns:
            A Response status.
        """
        with self.connect() as conn:
            try:
                res = conn.execute(text(query))
                return res.fetchall(), res.keys()

            except BaseException as e:
                raise e

    def conn_execute_df(self, query: str) -> pd.DataFrame:
        """Exeucete query and get dataframe result.

        Args:
            query: A query string that I want to execute.

        Returns:
            Result table in pandas dataframe after query executed.
        """
        with self.connect() as conn:
            try:
                res = conn.execute(text(query))
                res = pd.DataFrame(res.fetchall(), columns=res.keys())
                return res

            except BaseException as e:
                raise e

    def execute_query(self, conn, query: str) -> str:
        """Execute query with db connection.

        Args:
            query: A query string that I want to execute.

        Returns:
            A Response status.
        """
        try:
            res = conn.execute(text(query))
            return res.fetchall(), res.keys()

        except BaseException as e:
            raise e

    def execute_df(self, conn, query: str) -> pd.DataFrame:
        """Exeucete query and get dataframe result.

        Args:
            query: A query string that I want to execute.

        Returns:
            Result table in pandas dataframe after query executed.
        """
        try:
            res = conn.execute(text(query))
            res = pd.DataFrame(res.fetchall(), columns=res.keys())
            return res

        except BaseException as e:
            raise e

    def check_is_table(self, conn, table: str) -> bool:
        """Check if table exists in connected db.

        Args:
            table : Table name that I want to check if exists in database.

        Returns:
            A Boolean value that indicated if table exists or not.
        """
        try:
            query = """
                    SELECT 1 FROM {schema_name}.{table_name} LIMIT 1
                    """.format(
                schema_name=self.db, table_name=table
            )
            self.conn_execute_query(conn, query)
            print("table {} exists".format(self.table))
            return True

        except BaseException as e:
            print("table {} doesn't exist".format(self.table))
            return False
