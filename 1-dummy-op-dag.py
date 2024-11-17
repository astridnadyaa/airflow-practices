### --- Import Libraries ---
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.decorators import dag

### --- Default Arguments ---
default_args = {'owner': 'kribogoreng',
                'start_date': datetime(2024, 11, 11),
                'retries': 1}

### --- Define the DAG ---
@dag(
    default_args=default_args,
    description='Creating DAG using Dummy Operator',
    schedule_interval='@daily',
    catchup=False
)
def simple_dag():
    ### --- Define tasks ---
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    ### --- Define task dependencies ---
    start >> end
  
### --- Instantiate the DAG ---
dag = simple_dag()
