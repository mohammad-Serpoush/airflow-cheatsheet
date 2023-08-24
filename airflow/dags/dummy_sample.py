from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from callables.data_cleaner import data_cleaner
from callables.data_insert import insert_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


dag = DAG(
    dag_id="dummy_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)


t1 = BashOperator(task_id="print_1", bash_command="echo 1", dag=dag)
t2 = BashOperator(task_id="print_2", bash_command="echo 2", dag=dag)
t3 = BashOperator(task_id="print_3", bash_command="echo 3", dag=dag)
t4 = BashOperator(task_id="print_4", bash_command="echo 4", dag=dag)
t5 = BashOperator(task_id="print_5", bash_command="echo 5", dag=dag)
t6 = BashOperator(task_id="print_6", bash_command="echo 6", dag=dag)
t7 = BashOperator(task_id="print_7", bash_command="echo 7", dag=dag)
t8 = BashOperator(task_id="print_8", bash_command="echo 8", dag=dag)
t9 = BashOperator(task_id="print_9", bash_command="echo 9", dag=dag)

dummy_t = EmptyOperator(task_id="empty", dag=dag)


[t1, t2, t3, t4] >> dummy_t >> [t5, t6, t7, t8, t9]
