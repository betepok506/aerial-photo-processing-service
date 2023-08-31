import os
from airflow.models import Variable
from datetime import timedelta
from airflow.utils.email import send_email_smtp

LOCAL_DATA_DIR = Variable.get('local_data_dir')
LOCAL_MLRUNS_DIR = Variable.get('local_mlruns_dir')


def wait_for_file(file_name: str):
    return os.path.exists(file_name)


def error_callback(message):
    dag_run = message.get('dag_run')
    send_email_smtp(to=default_args['email'], subject=f'DAG {dag_run} has failed')


default_args = {
    'owner': 'Andrey Rotanov',
    'email': ['rotanov07@mail.ru'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': error_callback
}