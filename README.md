# AWS Redshift ETL Pipeline

This Python script is designed to set up an ETL (Extract, Transform, Load) pipeline on an AWS Redshift cluster. 
The pipeline involves creating tables, loading data into staging tables, and inserting data into the main tables. 
Additionally, it checks for the existence of data to avoid duplicating entries.

## Dependencies

Ensure you have the necessary Python packages installed:

```bash
pip install psycopg2 configparser boto3 
```

## Configuration

Create a `dwh.cfg` file with your AWS Redshift cluster configurations. The file should have the following structure:

```ini
[AWS]
KEY=<your_aws_access_key>
SECRET=<your_aws_secret_key>

[DWH]
HOST=<your_redshift_cluster_endpoint>
DB_NAME=<your_database_name>
DB_USER=<your_database_user>
DB_PASSWORD=<your_database_password>
DB_PORT=<your_database_port>
```

## Usage

The script is structured into different functions to perform specific tasks:

1. `set_up_redshift_cluster`: Creates a new AWS Redshift cluster and waits for it to become available.

2. `check_for_table_creation` and `check_for_table_drops`: Connects to the Redshift cluster, 
creates or drops tables, and closes the connection.

3. `check_for_data_insertion`: Connects to the Redshift cluster, loads staging tables, inserts data, 
and closes the connection.

4. `check_existing_data`: Connects to the Redshift cluster, checks if data already exists, and returns the result.

5. `main`: Main function to run the ETL pipeline. It initializes the Redshift cluster, checks for table creation, 
checks for data insertion, and provides information about the loaded data.

## Running the Script

Execute the script by running:

```bash
python nd_aws_data_warehouse/services/__init__.py
```

Ensure that your `dwh.cfg` file is properly configured.

The results of the ETL pipeline are printed to the console and shows if the expected results 
have been accomplished. The results are saved in a log file (`etl.log`). 


## Note

This script assumes that the necessary SQL queries for table creation, dropping, and ETL operations are defined 
in external files (`sql_queries.py` and others).

Feel free to modify the script to suit your specific requirements and customize the SQL queries accordingly.