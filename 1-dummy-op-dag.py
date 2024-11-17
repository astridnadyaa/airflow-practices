### --- Import Libraries ---
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.decorators import dag


### --- Default Arguments ---
default_args = {
    'owner': 'kribogoreng',
    'start_date': datetime(2024, 11, 11),
    'retries': 1
}


### -- Define the DAG ---
@dag(
    default_args=default_args,
    description='Download File using Bash Operator',
    schedule_interval='@weekly',
    catchup=False
)
def download_dag():
    ### --- Define Tasks ---
    start = DummyOperator(
        task_id='start'
    )

    download_file = BashOperator(
        task_id = 'download_file',
        bash_command=(
            'mkdir -p ${AIRFLOW_HOME}/downloads && '
            'wget -O ${AIRFLOW_HOME}/downloads/yelp.csv ' 
            '"https://www.dropbox.com/scl/fi/2k8im8ftu9yk8mnqhops9/yelp.csv'
            '?rlkey=52dzmxgys0su77wb6o75vb5ab&st=lmz21rpk&dl=0'
        )
    )

    end = DummyOperator(
        task_id = 'end'
    )

    ### --- Define dependencies ---
    start >> download_file >> end

### --- Instantiate the DAG ---
dag = download_dag()
