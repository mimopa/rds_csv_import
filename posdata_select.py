from dotenv import load_dotenv
import os
import mysql.connector as mydb

# 環境変数の設定
load_dotenv()

# コネクションの作成
rds = os.environ["RDS_JDMC"]

con = mydb.connect(
    host=rds,
    port=os.environ["RDS_JDMC_PORT"],
    user=os.environ["RDS_JDMC_USER"],
    password=os.environ["RDS_JDMC_PASSWORD"],
    database=os.environ["RDS_JDMC_DATABASE"],
    charset='utf8'
)

try:
    cur = con.cursor()

    sql = 'SELECT * FROM idpos_table_csv'
    cur.execute(sql)
    rows = cur.fetchall()
    for i in rows:
        print(i)

except mydb.Error as e:
    print("Error code:", e.errno)  # error number
    print("SQLSTATE value:", e.sqlstate)  # SQLSTATE value
    print("Error message:", e.msg)  # error message
    print("Error:", e)  # errno, sqlstate, msg values
    s = str(e)
    print("Error:", s)
