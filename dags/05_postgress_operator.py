from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {"owner": "coder2j", "retries": 5, "retry_delay": timedelta(minutes=5)}


with DAG(
    dag_id="dag_with_postgres_operator_v04",
    default_args=default_args,
    start_date=datetime(2021, 12, 19),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    task1 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            create table if not exists dag_runs_test (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """,
    )

    task2 = PostgresOperator(
        task_id="insert_into_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            insert into dag_runs_test (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """,
    )

    task3 = PostgresOperator(
        task_id="delete_data_from_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            delete from dag_runs_test where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';
        """,
    )
    task1 >> task3 >> task2