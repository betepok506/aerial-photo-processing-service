from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime
from time import sleep

with DAG(
    dag_id="simple_mapping",
    schedule=None,
    start_date=datetime(2022, 12, 22),
    catchup=False
) as dag:

    @task
    def read_conf():
        return [10, 20, 30]


    @task_group
    def calculations(x):

        @task
        def add_one(x: int):
            print(f"Я получил {x}, сложу его")
            sleep(x)
            return x + 1

        @task()
        def mul_two(x: int):
            print(f"Я получил {x}, умножу его")
            return x * 2

        mul_two(add_one(x))


    @task
    def pull_xcom(**context):

        pulled_xcom = context["ti"].xcom_pull(
            task_ids=['calculations.mul_two'],
            key="return_value"
        )
        print(pulled_xcom)


    calculations.expand(x=read_conf()) >> pull_xcom()