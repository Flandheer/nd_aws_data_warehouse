import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function will drop the tables in the Redshift cluster. The queries are stored in the sql_queries.py file.

    :param cur: Cursor to the Redshift cluster
    :param conn: Connection to the Redshift cluster
    :return: Not applicable
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function will create the tables in the Redshift cluster. The queries are stored in the sql_queries.py file.
    :param cur: Cursor to the Redshift cluster
    :param conn: Connection to the Redshift cluster
    :return: Not applicable
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()