import pandas as pd
import psycopg2
import os


def execute(command, cursor, connection):
    cursor.execute(command)
    connection.commit()


def fetch_execute(command, cursor):
    cursor.execute(command)
    return cursor.fetchone()


def fetch_all_execute(command, cursor):
    cursor.execute(command)
    return cursor.fetchall()


def sql_insert(command, args, cursor, connection, replace=False):
    duplicate = " ON CONFLICT (id) DO NOTHING;"
    if replace:
        duplicate = " ON CONFLICT (id) DO UPDATE;"
    #duplicate = ";"
    command += duplicate
    cursor.execute(command, args)
    connection.commit()


if __name__ == "__main__":
    conn = psycopg2.connect(host="localhost", dbname="test", user="postgres", password="password")
    cur = conn.cursor()
    print(f"Database version: {fetch_execute('SELECT version();', cur)}")

    table1exists = fetch_execute("SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'testTable' );", cur)[0]
    table2exists = fetch_execute("SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'testTable2' );", cur)[0]

    if not table1exists:
        execute("CREATE TABLE IF NOT EXISTS testTable (id bigint PRIMARY KEY, price varchar(5), doors int, safety varchar(4));", cur, conn)
    if not table2exists:
        execute("CREATE TABLE IF NOT EXISTS testTable2 (id bigint PRIMARY KEY, quality varchar(6));", cur, conn)

    CSV_FILE = "testdata.txt"

    columns = ["price", "maint", "doors", "persons", "lug_boot", "safety", "quality"]
    df = pd.read_csv(os.path.join(os.getcwd(), CSV_FILE), names=columns)

    #df1 = df.loc[:, ["price", "doors", "safety"]]
    #df2 = df.loc[:, ["quality"]]

    df.loc[df["doors"] == "5more", "doors"] = 5

    df = df.astype({"doors": int})

    df["id"] = df.index
    #df2["id"] = df2.index
    breakpoint()
    for (idx_label, row) in df.iterrows():
        command = "INSERT INTO testTable (id, price, doors, safety) VALUES (%s, %s, %s, %s)"
        args = (row['id'], row['price'], row['doors'], row['safety'])
        sql_insert(command, args, cur, conn)
        command = "INSERT INTO testTable2 (id, quality) VALUES (%s, %s)"
        args = (row["id"], row["quality"])
        sql_insert(command, args, cur, conn)

    ret = fetch_all_execute("SELECT (id, price, doors) FROM testTable WHERE doors <= 4 AND id % 10 = 0;", cur)

    print("Return\n-----------")
    for each in ret:
        print(each)

    cur.close()
    conn.close()
