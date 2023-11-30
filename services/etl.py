import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries




def load_staging_tables(cur, conn):
    """
    Staging tables are loaded from S3 to Redshift. The sql_queries.py file contains the queries to load the staging
    tables.
    :param cur: The cursor object for the database in the Redshift cluster
    :param conn: The connection object for the database in the Redshift cluster
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    The data from the staging tables are inserted in the datatables already created on the Redshift cluster.
    The sql_queries.py file contains the queries to retrieve the data from the staging tables and insert them in the
    fact and dimension tables.
    :param cur: The cursor object for the database in the Redshift cluster
    :param conn: The connection object for the database in the Redshift cluster
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def data_exists(cur):
    """
    Check if data is already loaded in the Redshift cluster. This can be used to review if loading the data is still
    necessary. Or to check if the data is loaded correctly.
    :param cur: The cursor object for the database in the Redshift cluster
    :return: A dictionary with the table names and the number of records in the table
    """
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

    print(*config['CLUSTER'].values())
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()