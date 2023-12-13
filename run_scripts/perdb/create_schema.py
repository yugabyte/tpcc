import psycopg2
import argparse

def create_tables(arg,i):
    conn = None
    try:
        if arg.pwd and len(arg.pwd) == 0:
            conn = psycopg2.connect("host={} dbname=perdb_{} user=yugabyte port=5433".format(arg.db, i))
        else:
            conn = psycopg2.connect(
                "host={} dbname=perdb_{} user=yugabyte password={} port=5433".format(arg.db, i, arg.pwd))
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        print("Creating table perdb_table in DB perdb_{}".format(i))
        cur.execute("CREATE TABLE perdb_table(col1 int, col2 int, primary key(col1))")
    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        conn.close()

def create_db(arg):
    try:
        if arg.pwd and len(arg.pwd) == 0:
            conn = psycopg2.connect("host={} dbname=yugabyte user=yugabyte port=5433".format(arg.db))
        else:
            conn = psycopg2.connect("host={} dbname=yugabyte user=yugabyte password={} port=5433".format(arg.db, arg.pwd))
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        for i in range(1, arg.num+1):
            print("Dropping database if exists perdb_{}".format(i))
            cur.execute("DROP DATABASE IF EXISTS perdb_{}".format(i))
            print("Creating database perdb_{}".format(i))
            cur.execute("CREATE DATABASE perdb_{}".format(i))
    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Enter the Yugabyte Cluster IP, Username, Number of databases to be created')
    parser.add_argument("-d", type=str, action="store", dest="db", required=True)
    parser.add_argument("-p", type=str, action="store", dest="pwd", required=False)
    parser.add_argument("-n", type=int, action="store", dest="num", required=True)
    args = parser.parse_args()
    create_db(args)
    for i in range(1, args.num + 1):
        create_tables(args, i)

if __name__ == "__main__":
    main()
