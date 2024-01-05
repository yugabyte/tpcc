import psycopg2
import argparse

def create_tables(arg):
    conn = None
    try:
        if arg.pwd and len(arg.pwd) == 0:
            conn = psycopg2.connect("host={} dbname=yugabyte user=yugabyte port=5433".format(arg.db))
        else:
            conn = psycopg2.connect(
                "host={} dbname=yugabyte user=yugabyte password={} port=5433".format(arg.db, arg.pwd))
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        for i in range(1, arg.num + 1):
            print("Creating table perdb_table_{} in DB yugabyte".format(i))
            cur.execute("CREATE TABLE perdb_table_{}(col1 int, col2 int, primary key(col1))".format(i))
    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Enter the Yugabyte Cluster IP, Username, Number of tables to be created in Yugabyte DB')
    parser.add_argument("-d", type=str, action="store", dest="db", required=True)
    parser.add_argument("-p", type=str, action="store", dest="pwd", required=False)
    parser.add_argument("-n", type=int, action="store", dest="num", required=True)
    args = parser.parse_args()
    create_tables(args)

if __name__ == "__main__":
    main()
