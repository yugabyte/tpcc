import time

import psycopg2
import argparse
import random
import threading

def run_ddl(arg):
    tbl_number = random.randint(1, arg.num)
    col_number = random.randint(3, 10)
    try:
        if arg.pwd and len(arg.pwd) == 0:
            conn = psycopg2.connect("host={} dbname=yugabyte user=yugabyte port=5433".format(arg.db))
            print("Connected to DB: yugabyte")
        else:
            conn = psycopg2.connect(
                "host={} dbname=yugabyte user=yugabyte password={} port=5433".format(arg.db, arg.pwd))
            print("Connected to DB: yugabyte")

        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute("SELECT column_name  FROM information_schema.columns WHERE table_name='perdb_table_{}' and column_name='col{}';".format(tbl_number,col_number))

        if cur.rowcount:
            print("ALTER TABLE perdb_table_{} DROP COLUMN col{} INTEGER;".format(tbl_number,col_number))
            cur.execute("ALTER TABLE perdb_table_{} DROP COLUMN col{};".format(tbl_number,col_number))
        else:
            print("ALTER TABLE perdb_table_{} ADD COLUMN col{} INTEGER;".format(tbl_number,col_number))
            cur.execute("ALTER TABLE perdb_table_{} ADD COLUMN col{} INTEGER;".format(tbl_number,col_number))

    except psycopg2.DatabaseError as error:
        print(error)

def main():
    parser = argparse.ArgumentParser(description='Enter the Yugabyte Cluster IP, Username, Number of tables available in YB')
    parser.add_argument("-d", type=str, action="store", dest="db", required=True)
    parser.add_argument("-p", type=str, action="store", dest="pwd", required=False)
    parser.add_argument("-n", type=int, action="store", dest="num", required=True)
    args = parser.parse_args()

    while True:
        run_ddl(args)
        time.sleep(50)

if __name__ == "__main__":
    main()
