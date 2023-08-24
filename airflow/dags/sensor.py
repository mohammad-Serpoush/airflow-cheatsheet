

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor


from callables.data_cleaner import data_cleaner
from callables.data_insert import insert_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


dag = DAG(
    dag_id="store_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=["/usr/local/airflow/sql-files"]
)


t1 = FileSensor(task_id="check_file_exist",
                filepath="/usr/local/airflow/raw-files/raw_store_transactions.csv",
                poke_interval=10,
                timeout=150,
                soft_fail=True,
                dag=dag)

t2 = PythonOperator(
    task_id="read_from_csv",
    python_callable=data_cleaner,
    dag=dag
)

t3 = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_conn",
    sql="create_table.sql",
    dag=dag
)

t4 = PythonOperator(
    task_id="insert_into_db",
    python_callable=insert_data,
    dag=dag
)
t1 >> t2 >> t3 >> t4

# t1 >> [t2 , t3] >> t4
