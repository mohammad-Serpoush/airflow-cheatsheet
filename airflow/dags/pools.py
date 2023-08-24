# First of all, create 2 pools in admin panel from Admin >> Pools


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 8, 23),
}

with DAG("pools", default_args=default_args, schedule_interval=timedelta(1)) as dag:
    t1 = BashOperator(task_id="task-1", bash_command="sleep 2", pool="pool_1")
    t2 = BashOperator(task_id="task-2", bash_command="sleep 2", pool="pool_1")
    t3 = BashOperator(task_id="task-3", bash_command="sleep 2", pool="pool_2")
    t4 = BashOperator(task_id="task-4", bash_command="sleep 2", pool="pool_2")
