import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def data_exists(cur):
    # Check if data is already loaded
    tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'times']
    count_records = {}

    for table in tables:
        cur.execute("SELECT COUNT(*) FROM {}".format(table))
        result = cur.fetchone()
        count_records[table] = result[0]

    return count_records

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()