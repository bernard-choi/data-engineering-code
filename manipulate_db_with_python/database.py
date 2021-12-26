"""
@author: yunsik choi
"""

from sqlalchemy import create_engine, text, MetaData
import pandas as pd


class Connection:
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


class Database(Connection):
    def __init__(
        self, host: str, user: str, password: str, db: str, port: str, table: str
    ):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.table = table

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
                return True

            except BaseException as e:
                raise e

    def conn_execute_df(self, query: str) -> pd.DataFrame:
        """Exeucete query and get dataframe result

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
        """Execute query with db connection inside connection

        Args:
            query: A query string that I want to execute.

        Returns:
            A Response status.
        """
        try:
            res = conn.execute(text(query))
            return True

        except BaseException as e:
            raise e

    def execute_df(self, conn, query: str) -> pd.DataFrame:
        """Exeucete query and get dataframe result inside connection

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

    def conn_check_is_table(self, table: str) -> bool:
        """Check if table exists in connected db.

        Args:
            table : Table name that I want to check if exists in database.

        Returns:
            A Boolean value that indicates if table exists or not.
        """
        with self.connect() as conn:
            try:
                meta_data = MetaData(bind=conn, reflect=True)
                tbl = meta_data.tables["{}".format(table)]
                print("table {} exists".format(table))
                return True

            except BaseException as e:
                print("table {} doesn't exist".format(table))
                return False

    def check_is_table(self, conn, table: str) -> bool:
        """Check if table exists in connected db inside connection

        Args:
            table : Table name that I want to check if exists in database.

        Returns:
            A Boolean value that indicates if table exists or not.
        """
        try:
            meta_data = MetaData(bind=conn, reflect=True)
            tbl = meta_data.tables["{}".format(table)]
            print("table {} exists".format(self.table))
            return True

        except BaseException as e:
            print("table {} doesn't exist".format(self.table))
            return False

    def conn_check_is_table(self, table: str) -> bool:
        with self.connect() as conn:
            try:
                meta_data = MetaData(bind=conn, reflect=True)
                tbl = meta_data.tables["{}".format(table)]
                print("table {} exists".format(table))
                return True

            except BaseException as e:
                print("table {} doesn't exist".format(table))
                return False

    def delete_all_insert_all(self, df: pd.DataFrame, db: str, table: str) -> bool:
        """Delete all table info and insert pandas dataframe

        Args:
            df: Data that are going to be inserted
            table: A target table name
            db: A target database name

        Returns:
             A Response status.
        """
        with self.connect() as conn:
            try:
                print("aa")
                with conn.begin():
                    # delete all data
                    self.execute_query(conn, "delete from {}.{}".format(db, table))
                    print("all deleted")
                    df.to_sql(
                        table,
                        con=conn,
                        if_exists="append",
                        index=False,
                        chunksize=5000,
                        method="multi",
                        schema=db,
                    )

            except Exception as e:
                print("{} delete and write failed, {}".format(table, e))
                return False
        return True
