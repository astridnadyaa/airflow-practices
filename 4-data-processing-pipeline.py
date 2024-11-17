### --- Import Libraries ---
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta
import pandas as pd
import sqlite3 

### --- Path ---
ORIGINAL_DATA = '/Users/astridnadyaa/apache-airflow/datasets/ecommerce_marketing.csv'
CLEANED_DATA = '/Users/astridnadyaa/apache-airflow/source-data/ecommerce_marketing_cleaned.csv'
AGGREGATED_DATA = '/Users/astridnadyaa/apache-airflow/output-data/ecommerce_marketing_aggregated.csv'

SQLITE_CONN_ID = 'my_sqlite_conn'


### --- Default Arguments ---
default_args = {
    'owner': 'kribogoreng',
    'retries': 1
}

### --- Functions ---
def remove_null_values():
    df = pd.read_csv(ORIGINAL_DATA)
    df = df.dropna()
    print(df)
    df.to_csv(CLEANED_DATA, index=False)

def aggregate_data():
    df = pd.read_csv(CLEANED_DATA)
    aggregated_df = df.groupby(['Gender', 'Product', 'Category'])['Amount'].mean().reset_index()
    aggregated_df = aggregated_df.sort_values(by='Amount', ascending=False)
    print(aggregated_df)
    aggregated_df.to_csv(AGGREGATED_DATA, index=False)

def insert_into_sqlite():
    connection = BaseHook.get_connection(SQLITE_CONN_ID)
    conn = sqlite3.connect(connection.host)
    cursor = conn.cursor()

    with open(AGGREGATED_DATA, 'r') as f:
        # Skip header
        next(f)
        for line in f:
            gender, product, category, avg_amount = line.strip().split(',')
            cursor.execute(
                "INSERT INTO aggregated_ecommerce_data (Gender, Product, Category, AvgAmount) VALUES (?, ?, ?, ?)",
                (gender, product, category, avg_amount)
            )
    
    conn.commit()
    conn.close()


### --- Define the DAG ---
with DAG(
    dag_id='data_pipeline',
    description='Data Processing Pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = f'test -f {ORIGINAL_DATA} || exit 1'
        )
    
    clean_data = PythonOperator(
        task_id = 'clean_data',
        python_callable=remove_null_values
    )

    aggregated_data = PythonOperator(
        task_id = 'aggregated_data',
        python_callable=aggregate_data
    )

    drop_table_if_exists = SqliteOperator(
        task_id = 'drop_table_if_exists',
        sqlite_conn_id = SQLITE_CONN_ID,
        sql = """
            DROP TABLE IF EXISTS aggregated_ecommerce_data;
        """
    )

    create_table = SqliteOperator(
        task_id = 'create_table',
        sqlite_conn_id = SQLITE_CONN_ID,
        sql = """
            CREATE TABLE aggregated_ecommerce_data (
                Gender TEXT,
                Product TEXT,
                Category TEXT,
                AvgAmount FLOAT
            )
        """
    )

    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable=insert_into_sqlite
    )


### --- Task Dependencies ---
check_file_exists >> clean_data >> aggregated_data >> drop_table_if_exists >> create_table >> load_data