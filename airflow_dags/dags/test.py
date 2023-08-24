# from datetime import timedelta, datetime
#
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from docker.types import Mount
# from airflow.providers.docker.operators.docker import DockerOperator
# from utils import LOCAL_DATA_DIR, default_args
#
#
# with DAG(
#         'test dag',
#         default_args=default_args,
#         schedule_interval='@daily',
#         start_date=datetime(2022, 11, 28)
# ) as dag:
#     wait_data = PythonSensor(
#         task_id='wait-for-predict-data',
#         python_callable=wait_for_file,
#         op_args=['/opt/airflow/data/raw/{{ ds }}/data.csv'],
#         timeout=6000,
#         poke_interval=10,
#         retries=10,
#         mode="poke"
#     )
#
#     generate_data = DockerOperator(
#         image='airflow-generate-data',
#         command='--output-dir /data/raw/{{ ds }}',
#         network_mode='bridge',
#         task_id='docker-airflow-generate-data',
#         do_xcom_push=False,
#         auto_remove=True,
#         mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
#     )
#
#     generate_data