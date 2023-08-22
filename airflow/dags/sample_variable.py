from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from callables.data_cleaner import data_cleaner
from callables.data_insert import insert_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


dag = DAG(
    dag_id="sample_variable",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)


def write_var_to_file(ti, var, *args, **kwargs):
    with open("/usr/local/airflow/clean-files/sample.txt", "w")as f:
        f.write(Variable.get("NAME"))


t1 = PythonOperator(
    task_id="write_var_to_file",
    python_callable=write_var_to_file,
    provide_context=True,
    dag=dag,
)


t2 = BashOperator(
    task_id="write_var_to_file_bash",
    bash_command="echo {{var.value.NAME}}",
    dag=dag,
)


t1 >> t2
