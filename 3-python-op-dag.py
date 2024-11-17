### --- Import Libraries ---
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

from datetime import datetime
import pandas as pd


### --- Default Arguments ---
default_args = {
    'owner': 'kribogoreng',
    'start_date': datetime(2024, 11, 11),
    'retries': 1
}

### --- Define the DAG ---
@dag(
    default_args=default_args,
    description='Creating DAG using Python and Bash Operator',
    schedule_interval='@daily',
    catchup=False
)

### --- Create Data Pipeline ---
def data_pipeline():
    ### --- Task 1 ---
    donwload_file = BashOperator(
        task_id='download_file',
        bash_command='mkdir -p ${AIRFLOW_HOME}/downloads && wget -O ${AIRFLOW_HOME}/downloads/yelp.csv https://www.dropbox.com/scl/fi/2k8im8ftu9yk8mnqhops9/yelp.csv?rlkey=52dzmxgys0su77wb6o75vb5ab&st=lmz21rpk&dl=0'
    )

    ### --- Task 2 ---
    def process_data():
        import os
        base_path = os.environ['AIRFLOW_HOME']
        df = pd.read_csv(f'{base_path}/downloads/yelp.csv')
        df_filtered = df[['text', 'stars']]
        df_filtered.to_csv(f'{base_path}/downloads/processed_yelp.csv', index=False)
    
    process_file = PythonOperator(
        task_id='process_file',
        python_callable=process_data
    )

    ### --- Task 3 ---
    cleanup_file = BashOperator(
        task_id='cleanup',
        bash_command='rm -rf ${AIRFLOW_HOME}/downloads/processedyelp.csv'
    )

    ### --- DAG Workflow ---
    donwload_file >> process_file >> cleanup_file

### --- Call Data Pipeline ---
dag = data_pipeline()