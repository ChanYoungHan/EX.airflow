from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {"owner": "coder2j", "retries": 5, "retry_delay": timedelta(minutes=5)}


def greet(some_dict, ti, execution_date, next_ds):
    print("some dict: ", some_dict)
    first_name = ti.xcom_pull(task_ids="get_names", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_names", key="last_name")
    age = ti.xcom_pull(task_ids="get_ages", key="age")
    print(
        f"Hello World! My name is {first_name} {last_name}, "
        f"and I am {age} years old!"
    )
    print(f"Excution data : {execution_date}, next_date: {next_ds}")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Jerry")
    ti.xcom_push(key="last_name", value="Fridman")


def get_age(ti):
    ti.xcom_push(key="age", value=19)


with DAG(
    default_args=default_args,
    dag_id="our_dag_with_python_operator_v09",
    description="Our first dag using python operator",
    start_date=datetime(2021, 10, 6),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={"some_dict": {"a": 1, "b": 2}},
    )

    task2 = PythonOperator(task_id="get_names", python_callable=get_name)

    task3 = PythonOperator(task_id="get_ages", python_callable=get_age)

    [task2, task3] >> task1
