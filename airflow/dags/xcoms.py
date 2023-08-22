import random
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

args = {
    "owner": "Airflow",
    "start_date": datetime.now() - timedelta(1),
}

dag = DAG(dag_id="simple_xcom",
          default_args=args, schedule_interval="@daily")


def generate_random_number(ti, **kwargs):
    num = random.randint(1, 10)
    ti.xcom_push(key="number", value=num)


def print_based_on_even_or_odd(ti, **kwargs):
    with open("/usr/local/airflow/clean-files/xcom_result.txt", "w") as f:
        num = ti.xcom_pull(key="number")
        if not num:
            f.write("No Num")    

        f.write(str(num))


t1 = PythonOperator(
    task_id="generate_number",
    python_callable=generate_random_number,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id="get_result",
    python_callable=print_based_on_even_or_odd,
    provide_context=True,
    dag=dag,
)
t1 >> t2
