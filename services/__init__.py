# Standard python packages
import psycopg2
import configparser
import time

# Custom python packages
from redshift import RedshiftCluster
from sql_queries import create_table_queries, drop_table_queries
from create_tables import create_tables, drop_tables
from etl import insert_tables, load_staging_tables, data_exists

def set_up_redshift_cluster(redshift_cluster):
    """
    Set up a Redshift cluster and wait for it to be available. The cluster properties are stored in the dwh.cfg file.
    :param redshift_cluster:
    :return: Not applicable
    """

    # Create a Redshift cluster
    redshift_cluster.create_redshift_cluster()

    # wait for the redshift cluster to be available
    while True:
        redshift_cluster_props = redshift_cluster.get_redshift_cluster_props()
        cluster_status = redshift_cluster_props['Clusters'][0]['ClusterStatus']
        if cluster_status == 'available':
            print(f"\n Redshift cluster is available!\n {redshift_cluster_props}")
            break
        else:
            print(f"\n Redshift cluster is not available yet!\n Status: {cluster_status}")
        time.sleep(60)

def check_for_table_creation(props):
    """
    This check will create the tables in the Redshift cluster.
    :param props: Redshift cluster properties
    :return:
    """
    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*props))
    cur = conn.cursor()

    create_tables(cur, conn)

    # Close connection to the Redshift cluster
    conn.close()

def check_for_table_drops(props):
    """
    This check will create the tables in the Redshift cluster. Seperate in case we want to drop tables before
    creating them. In some cases the tables have been create but the data is not loaded yet. Therefore we want to
    keep the tables that are already created. Therefore we have a seperate check for dropping tables.
    :param props: Redshift cluster properties
    :return:
    """
    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*props))
    cur = conn.cursor()

    drop_tables(cur, conn)

    # Close connection to the Redshift cluster
    conn.close()

def check_for_data_insertion(props):
    """
    This check will insert data into the tables in the Redshift cluster.
    :param props: Redshift cluster properties
    :return:
    """

    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*props))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close connection to the Redshift cluster
    conn.close()

def check_existing_data(props):
    """
    This check will check if data is already loaded in the tables in the Redshift cluster. It returns a dictionary
    with the table names and the number of records in the table. This gives an indication if the data is loaded.
    :param props: Redshift cluster properties
    :return: Dictionary with table names and number of records
    """
    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*props))
    cur = conn.cursor()

    if_data_exists = data_exists(cur)

    # Close connection to the Redshift cluster
    conn.close()

    return if_data_exists

def check_result_of_data_insertion(data_stored):
    """
    Check if data is already loaded according to expected results
    :param data_stored: The data stored by the ETL pipeline
    :return: True if the data is loaded, False if the data is not loaded
    """

    expected_results = {'staging_events': 8056, 'staging_songs': 14896, 'songplays': 1144, 'users': 107, 'songs': 14896,
                        'artists': 10025, 'times': 8023}

    return expected_results == data_stored

def main():
    """
    Main function to run the ETL pipeline
    :return:
    """

    print("Initializing Redshift cluster...")
    redshift_cluster = RedshiftCluster()
    get_redshift_cluster_props = redshift_cluster.get_redshift_cluster_props()

    if not get_redshift_cluster_props:
        print("Setting up a new server on the cluster")
        set_up_redshift_cluster(redshift_cluster)

    print(redshift_cluster)

    # The cluster pops are stored in the dwh.cfg file. Improve by retrieving the props from the Redshift cluster
    props = [redshift_cluster.HOST,
             redshift_cluster.DB_NAME,
             redshift_cluster.DB_USER,
             redshift_cluster.DB_PASSWORD,
             redshift_cluster.DB_PORT]

    # Drop existing table and create new tables
    print(f"\n Checking for table creation...")
    check_for_table_drops(props)
    check_for_table_creation(props)

    print(f"\n Checking for data insertion...")
    # Check if data is already loaded
    if_data_exists = check_existing_data(props)
    print(f"\n Data already loaded: {if_data_exists}")

    # sum the values in the dictionary of the keys 'songplays', 'users', 'songs', 'artists', 'times'
    if_data_exists_sum = sum(
        if_data_exists[key_dict] for key_dict in ['songplays', 'users', 'songs', 'artists', 'times'])
    if if_data_exists_sum == 0:
        print(f"\n Not all data loaded yet. Loading data...")
        start_time = time.time()
        check_for_data_insertion(props)
        end_time = time.time()
        print(f"\n Time taken to load data: {end_time - start_time} seconds")

    # Check if data is already loaded
    data_stored = check_existing_data(props)

    # Print results
    database_statement = f"\n========== Redshift database ==========\n" \
                         f"\n Fact table songplays: {data_stored['songplays']}"\
                         f"\n Dimension table users: {data_stored['users']}"\
                         f"\n Dimension table songs: {data_stored['songs']}"\
                         f"\n Dimension table artists: {data_stored['artists']}"\
                         f"\n Dimension table times: {data_stored['times']}"
    print(database_statement)

    # Test if data is loaded
    test_statement = f"\n========== Test results ==========\n"\
                     f"\n Goal: Load data into the Redshift cluster and store it in the database"\
                     f"\n Result: {len(data_stored.keys())} out of 7 tables loaded " \
                     f"\n Test passed: {check_result_of_data_insertion(data_stored)}"
    print(test_statement)

    # save result in a file (etl.log)
    with open('etl.log', 'w') as f:
        f.write(f"\n============ Redshift Cluster ============\n" \
                f"RedshiftClusterEndPoint={redshift_cluster.HOST}\n" \
                f"RedshiftIdentifier={redshift_cluster.DB_IDENTIFIER}\n" \
                f"RedshiftUser={redshift_cluster.DB_USER}\n" \
                f"RedshiftIAMRole={redshift_cluster.ARN}")
        f.write(database_statement)
        f.write(test_statement)

if __name__ == "__main__":
    main()

