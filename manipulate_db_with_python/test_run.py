from database import Database

db = Database(host="", user="", password="", db="", port="")


if __name__ == "__main__":
    query = """
    select * from db.table limit 1
    """
    result = db.execute_df(query)
    print(result)

# id name
# 1 yunsik
# 2 choi
# ...
