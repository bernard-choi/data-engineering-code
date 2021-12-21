"""
@author: yunsik choi
"""

from sqlalchemy import create_engine, text
import pandas as pd

from database import DataBase


class BulkUpdate(DataBase):
    def __init__(self, table, **kwargs):
        self.table = table

    def bulk_update_rows(self, json_input: list, join_key: list, create_mode: int = 0):
        """Bulk update by creating temp table and join update in SQL.

        Args:
            json_input: List of dict that has update information
                ex) json_input = [{'id': 1, 'column_1': 'value_1', 'column_2': 'value_2'},
                                  {'id': 1, 'column_1': 'value_1', 'column_2': 'value_2'},]

            join_key: Join key columns that are needed for join update
                ex) join_key = ['column1', 'columns2']

            create_mode: An option that stop function(=0) or not(=1) if there if no temp_table


        Returns:
            A Boolean value of update status
        """

        df = pd.DataFrame(json_input)
        df_col = df.columns

        if join_key not in df_col:
            return "join key not exists"

        list_dict_input = df[df.columns.difference([join_key])].to_dict(
            orient="records"
        )
        list_dict_condition = df[[join_key]].to_dict(orient="records")

        set_conditions = ",".join(
            [
                "A.{key} = B.{key}".format(key=key)
                for key, value in list_dict_input[0].items()
            ]
        )
        join_conditions = ",".join(
            [
                "A.{key} = B.{key}".format(key=key)
                for key, value in list_dict_condition[0].items()
            ]
        )

        join_query = """
                update {db}.{table} A
                inner join {db}.{table_dummy} B 
                on {join_conditions}
                set {set_conditions}
                """.format(
            db=self.db,
            table=self.table,
            table_dummy=self.table + "_dummy",
            join_conditions=join_conditions,
            set_conditions=set_conditions,
        )

        with self.connect() as conn:
            try:
                # transaction
                with conn.begin():
                    # dummy 테이블이 있다면 delete, insert
                    if self.check_is_table(conn, table=self.table + "_dummy"):
                        self.execute_query(
                            conn,
                            "delete from {}.{}".format(self.db, self.table + "_dummy"),
                        )
                        df.to_sql(
                            self.table + "_dummy",
                            schema=self.db,
                            con=conn,
                            if_exists="append",
                            index=False,
                            chunksize=300,
                            method="multi",
                        )

                    else:
                        if create_mode == 0:
                            return False
                        else:
                            # dummy table이 없고, create_mode가 1이면 create (if_exists='replace')
                            df.to_sql(
                                self.table + "_dummy",
                                schema=self.db,
                                con=conn,
                                if_exists="replace",
                                index=False,
                                chunksize=300,
                                method="multi",
                            )
                            print("dummy table 생성 완료")
                    print("dummy table 업데이트 완료")
                    ##  join update
                    self.execute_query(conn, join_query)

            except Exception as e:
                print("{} update failed, {}".format(self.table, e))
                return False

        print("update completed")
        return True
