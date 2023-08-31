from datetime import datetime
import uuid
import pandas as pd
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from utils import LOCAL_MLRUNS_DIR, LOCAL_DATA_DIR, default_args, wait_for_file
from airflow.decorators import task, task_group
from airflow.operators.python import get_current_context

# TODO: Передать уникальный id для идентификации загрузки дага!
with DAG(
        'image_loading_and_processing',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 8, 31),
        render_template_as_native_obj=True,
        params={
            "prefix_file_online_storage": "./data/images_online_storage_",
            "suffix_file_online_storage": ".csv",
            "prefix_file_lta_storage": "./data/images_lts_storage_",
            "suffix_file_lta_storage": ".csv",
        }
) as dag:
    @task
    def initializing_parameters():
        context = get_current_context()
        dag_id = str(uuid.uuid4())
        context["ti"].xcom_push(key='dag_id', value=dag_id)
        path_online_storage_image_file = f"{context['params']['prefix_file_online_storage']}" + dag_id + \
                                         f"{context['params']['suffix_file_online_storage']}"
        context["ti"].xcom_push(key='path_online_storage_image_file', value=path_online_storage_image_file)

        path_lta_storage_image_file = f"{context['params']['prefix_file_lta_storage']}" + dag_id + \
                                      f"{context['params']['suffix_file_lta_storage']}"
        context["ti"].xcom_push(key='path_lta_storage_image_file', value=path_lta_storage_image_file)


    @task
    def search_images():
        # TODO: Передать уникальный id для идентификации загрузки дага!
        context = get_current_context()

        DockerOperator(
            image='airflow-copernicus-api/search-images',
            command='search-images.py',
            network_mode='bridge',
            task_id='docker-airflow-search-images',
            # do_xcom_push=False,
            docker_url="unix://var/run/docker.sock",
            auto_remove=True,
            environment={
                "PATH_ONLINE_STORAGE_IMAGE_FILE": context["ti"].xcom_pull(
                    task_ids="initializing_parameters",
                    key="path_online_storage_image_file"),

                "PATH_LTA_STORAGE_IMAGE_FILE": context["ti"].xcom_pull(
                    task_ids="initializing_parameters",
                    key="path_lta_storage_image_file"),
            },
            mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
        ).execute(context=context)


    wait_for_online_storage_data = PythonSensor(
        task_id='wait-for-online-storage-data',
        python_callable=wait_for_file,
        op_args=[
            '{{ task_instance.xcom_pull(task_ids="initializing_parameters", key="path_online_storage_image_file") }}'],
        timeout=6000,
        poke_interval=10,
        retries=10,
        mode="poke"
    )

    wait_for_lta_storage_data = PythonSensor(
        task_id='wait-for-lta-storage-data',
        python_callable=wait_for_file,
        op_args=[
            '{{ task_instance.xcom_pull(task_ids="initializing_parameters", key="path_lta_storage_image_file") }}'],
        timeout=6000,
        poke_interval=10,
        retries=10,
        mode="poke"
    )


    @task
    def scheduler_image_id():
        context = get_current_context()
        filename = context["ti"].xcom_pull(
            task_ids="initializing_parameters",
            key="path_online_storage_image_file")
        data = pd.read_csv(filename)
        print(f'Data !!!!!!!!!!!!!: {len(data)}')
        result = []
        for ind, row in data.iterrows():
            result.append({'image_id': row["image_id"],
                           'filename': row['filename']})
            # result.append(row["image_id"])
        print(result)
        return result


    @task_group
    def processing_images(name_folder):
        # Внедрить закачку
        @task
        def download_image(meta_info_image):
            context = get_current_context()
            print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! {meta_info_image}")
            image_id = meta_info_image["image_id"]
            filename = meta_info_image["filename"]
            DockerOperator(
                image='airflow-copernicus-api/download-images',
                command=f'download-image.py',
                network_mode='bridge',
                task_id='docker-airflow-download-images',
                # do_xcom_push=False,
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
                environment={
                    "IMAGE_ID": image_id
                },
                mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
            ).execute(context=context)
            return image_id

        @task
        def merge(folder):
            context = get_current_context()

            DockerOperator(
                image='airflow-osgeo-gdal',
                command='gdalbuildvrt -separate'
                        f'/data/stack_{folder}.vrt'
                        f'/data/{folder}/{folder}_SR_B7.TIF '
                        f'/data/{folder}/{folder}_SR_B6.TIF '
                        f'/data/{folder}/{folder}_SR_B2.TIF '
                        '-separate',
                network_mode='bridge',
                task_id='docker-airflow-merge-preprocess',
                # do_xcom_push=False,
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
                mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
            ).execute(context=context)
            return folder

        @task
        def create_rgb(folder):
            context = get_current_context()

            DockerOperator(
                image='airflow-osgeo-gdal',
                command='gdal_translate -scale -ot Byte '
                        f'/data/output_{folder}.tif  '
                        f'/data/output_8bit_{folder}.tif',
                network_mode='bridge',
                task_id='docker-airflow-create-rgb',
                # do_xcom_push=False,
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
                mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
            ).execute(context=context)

        @task
        def create_tiles(folder):
            context = get_current_context()

            DockerOperator(
                image='airflow-osgeo-gdal',
                command='gdal2tiles.py '
                        '-z 7-9 '
                        '-w none '
                        f'/data/output_8bit_{folder}.tif  '
                        f'/data/tilesdir_yrrraaa_{folder}',

                network_mode='bridge',
                task_id='docker-airflow-create-tiles',
                # do_xcom_push=False,
                docker_URL="//var/run/docker.sock",
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
                mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
            ).execute(context=context)

        create_tiles(folder=create_rgb(folder=merge(folder=download_image(name_folder))))


    list_images = scheduler_image_id()
    initializing_parameters() >> search_images() >> [wait_for_online_storage_data,
                                                     wait_for_lta_storage_data] >> list_images

    processing_images.expand(name_folder=list_images)
#