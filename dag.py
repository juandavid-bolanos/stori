from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'stori_challenge',
    default_args=default_args,
    description='A DAG to run Stori challenge scripts',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
def run_script(script_name):
    os.system(f'python {script_name}')

t1 = BashOperator(
    task_id='install_dependencies',
    bash_command='pip install -r requirements.txt',
    dag=dag,
)

t2 = PythonOperator(
    task_id='run_sql_table',
    python_callable=run_script,
    op_args=['sql_table.py'],
    dag=dag,
)

t3 = PythonOperator(
    task_id='run_sql_etl',
    python_callable=run_script,
    op_args=['sql_etl.py'],
    dag=dag,
)

t4 = PythonOperator(
    task_id='run_nosql_collection',
    python_callable=run_script,
    op_args=['nosql_collection.py'],
    dag=dag,
)

t5 = PythonOperator(
    task_id='run_nosql_etl',
    python_callable=run_script,
    op_args=['nosql_etl.py'],
    dag=dag,
)

t6 = PythonOperator(
    task_id='run_dw_consolidation',
    python_callable=run_script,
    op_args=['dw_consolidation.py'],
    dag=dag,
)

# Define the task dependencies
t1 >> t2 >> t3 >> t4 >> t5 >> t6
