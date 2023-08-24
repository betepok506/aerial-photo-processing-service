from datetime import datetime
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from utils import LOCAL_MLRUNS_DIR, LOCAL_DATA_DIR, default_args, wait_for_file
from airflow.decorators import task, task_group

with DAG(
        'image_loading_and_processing',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 8, 21)
) as dag:
    @task
    def getting_snapshot_dictionary():
        return ["LC08_L2SP_131015_20210729_20210804_02_T1",
                "LC08_L2SP_131016_20210729_20210804_02_T1",
                "LC09_L2SP_168027_20230612_20230614_02_T1"]


    @task_group
    def processing_images(name_folder):
        # DockerOperator().expand()
        @task
        def printing(name_folder):
            print(f"Cur name folder {name_folder}")
            return name_folder

        merge_channel = DockerOperator(
            image='osgeo/gdal',
            command='gdal_merge.py -o ' 
                    f'/data/output_1.tif '
                    f'/data/LC08_L2SP_131016_20210729_20210804_02_T1/LC08_L2SP_131016_20210729_20210804_02_T1_SR_B7.TIF '
                    f'/data/LC08_L2SP_131016_20210729_20210804_02_T1/LC08_L2SP_131016_20210729_20210804_02_T1_SR_B6.TIF '
                    f'/data/LC08_L2SP_131016_20210729_20210804_02_T1/LC08_L2SP_131016_20210729_20210804_02_T1_SR_B2.TIF '
                    '-separate',
            network_mode='bridge',
            task_id='docker-airflow-merge-preprocess',
            # do_xcom_push=False,
            docker_url="unix://var/run/docker.sock",
            # auto_remove=True,
            mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
        )

        create_rgb = DockerOperator(
            image='osgeo/gdal',
            command='-scale -ot Byte '
                    f'/data/output_{name_folder}.tif  '
                    f'/data/output_8bit_{name_folder}.tif',
            network_mode='bridge',
            task_id='docker-airflow-create-rgb',
            # do_xcom_push=False,
            docker_url="unix://var/run/docker.sock",
            # auto_remove=True,
            mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
        )

        create_tiles = DockerOperator(
            image='osgeo/gdal',
            command='gdal2tiles.py '
                    '-z 7-12 '
                    '-w none '
                    f'/data/output_8bit_{name_folder}.tif  '
                    f'/data/tilesdir_yrrraaa_{name_folder}',

            network_mode='bridge',
            task_id='docker-airflow-create-tiles',
            # do_xcom_push=False,
            # docker_URL="//var/run/docker.sock",
            docker_url="unix://var/run/docker.sock",
            # auto_remove=True,
            mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
        )
        printing >> merge_channel >> create_rgb >> create_tiles


    # wait_data = PythonSensor(
    #     task_id='wait-for-predict-data',
    #     python_callable=wait_for_file,
    #     op_args=['/opt/airflow/data/raw/{{ ds }}/data.csv'],
    #     timeout=6000,
    #     poke_interval=10,
    #     retries=10,
    #     mode="poke"
    # )
    #
    # preprocess = DockerOperator(
    #     image='airflow-preprocess',
    #     command='--input-dir /data/raw/{{ ds }}/ '
    #             '--output-dir /data/processed/{{ ds }}/ ',
    #     network_mode='bridge',
    #     task_id='docker-airflow-predict-preprocess',
    #     do_xcom_push=False,
    #     auto_remove=True,
    #     mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
    # )
    #
    # predict = DockerOperator(
    #     image='airflow-predict',
    #     command='--path-to-data /data/processed/{{ ds }} '
    #             '--output-dir /data/predictions/{{ ds }}',
    #     network_mode='host',
    #     task_id='docker-airflow-predict',
    #     do_xcom_push=False,
    #     auto_remove=True,
    #     mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind'),
    #             Mount(source=LOCAL_MLRUNS_DIR, target='/mlruns', type='bind')]
    # )

    processing_images.expand(name_folder=getting_snapshot_dictionary())
    # wait_data >> preprocess >> predict
