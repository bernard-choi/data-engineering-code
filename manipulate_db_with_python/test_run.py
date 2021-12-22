import pandas as pd

from database import Database
from bulk_update import Bulkupdate

# Get Connection

db = Database(host="", user="", password="", db="", port="")
db2 = Bulkupdate(host="", user="", password="", db="", port="", table="")

## 1. execute query and get results in pandas dataframe

query = """
select * from db.table limit 1
"""
result = db.execute_df(query)
print(result)

# id name
# 1 yunsik
# 2 choi
# ...


## 2. bulk update with join key and set_values

# raw table
# id   join_column1   join_column2	  update_column
# 1	        1	           1	           1
# 2	        2	           2	           2
# 3	        3	           3	           3

dict_input = [
    {"id": 1, "join_column1": 1, "join_column2": 1, "update_column": 10},
    {"id": 2, "join_column1": 2, "join_column2": 2, "update_column": 20},
    {"id": 3, "join_column1": 3, "join_column2": 3, "update_column": 30},
]
join_key = ["join_column1", "join_column2"]

db.bulk_update_rows(dict_input, join_key, create_mode=1)

# updated table
# id   join_column1   join_column2	  update_column
# 1	        1	           1	           10
# 2	        2	           2	           20
# 3	        3	           3	           30
