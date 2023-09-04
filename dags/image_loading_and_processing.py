import os
from datetime import datetime
import uuid
import pandas as pd
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from utils import (
    # LOCAL_MLRUNS_DIR,
    LOCAL_DATA_DIR,
    default_args, wait_for_file, CreatePoolOperator,
    NUM_PARALLEL_SENTINEL_DOWNLOADS,
    NUM_PARALLEL_SENTINEL_IMAGE_PROCESSING)
from airflow.decorators import task, task_group
from airflow.operators.python import get_current_context

# Создание пула закачки снимков

# CreatePoolOperator('downloading_sentinels', 4)

# TODO: Передать уникальный id для идентификации загрузки дага!
with DAG(
        'image_loading_and_processing',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 9, 4),
        render_template_as_native_obj=True,
        params={
            "prefix_local_storage_path": './data/',
            "prefix_file_online_storage": "images_online_storage_",
            "suffix_file_online_storage": ".csv",
            "prefix_file_lta_storage": "images_lts_storage_",
            "suffix_file_lta_storage": ".csv",
            "bands": [2, 3, 4],
            "minimum_tile_zoom": 7,
            "maximum_tile_zoom": 9,
            "spatial_expansion": 10
        }
) as dag:

    @task
    def initializing_parameters():
        import os
        context = get_current_context()
        dag_id = str(uuid.uuid4())
        context["ti"].xcom_push(key='dag_id', value=dag_id)
        path_online_storage_image_file = os.path.join(f"{context['params']['prefix_local_storage_path']}",
                                                      f"{context['params']['prefix_file_online_storage']}" + dag_id + \
                                                      f"{context['params']['suffix_file_online_storage']}")
        context["ti"].xcom_push(key='path_online_storage_image_file', value=path_online_storage_image_file)

        path_lta_storage_image_file = os.path.join(f"{context['params']['prefix_local_storage_path']}",
                                                   f"{context['params']['prefix_file_lta_storage']}" + dag_id + \
                                                   f"{context['params']['suffix_file_lta_storage']}")
        context["ti"].xcom_push(key='path_lta_storage_image_file', value=path_lta_storage_image_file)

        CreatePoolOperator(
            slots=NUM_PARALLEL_SENTINEL_DOWNLOADS,
            name='downloading_sentinels',
            task_id='create_pool_sentinel_downloads'
        ).execute(context)

        CreatePoolOperator(
            slots=NUM_PARALLEL_SENTINEL_IMAGE_PROCESSING,
            name='image_preprocessing',
            task_id='create_pool_image_preprocessing'
        ).execute(context)


    @task
    def search_images():
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

                "USER_COPERNICUS_LOGIN": os.getenv("USER_COPERNICUS_LOGIN"),

                "USER_COPERNICUS_PASSWORD": os.getenv("USER_COPERNICUS_PASSWORD"),
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
        result = []
        for ind, row in data.iterrows():
            tmp_dict = {}
            for k in row.keys():
                tmp_dict[k] = row[k]

            result.append(tmp_dict)

        return result


    @task_group
    def processing_images(name_folder):

        @task(pool='downloading_sentinels', pool_slots=1)
        def download_image(meta_info_image):
            context = get_current_context()
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

            return filename

        @task(pool='image_preprocessing', pool_slots=1)
        def unzipping(filename):
            import zipfile
            import os
            print(f'INPUT: {filename}')
            context = get_current_context()
            unzipped_file = os.path.join(context['params']['prefix_local_storage_path'],
                                         filename.split('.')[0] + '.zip')
            with zipfile.ZipFile(unzipped_file, 'r') as zip_ref:
                zip_ref.extractall(context['params']['prefix_local_storage_path'])

            return filename

        @task(pool='image_preprocessing', pool_slots=1)
        def extracting_meta_information(filename):
            from bs4 import BeautifulSoup
            import glob
            import os
            print(f"filename: {filename}")
            context = get_current_context()
            bands = context['params']['bands']
            spatial_expansion = context['params']['spatial_expansion']
            prefix_local_storage_path = context["params"]['prefix_local_storage_path']

            path_to_file = os.path.join(prefix_local_storage_path,
                                        filename, 'MTD*')
            xml_search_result = glob.glob(path_to_file)
            assert len(xml_search_result) != 0, "The file with the information was not found!"
            path_to_xml_file = xml_search_result[0]

            print(f"bands: {bands}")
            print(f"spatial_expansion: {spatial_expansion}")
            print(f"path_to_xml_file: {path_to_xml_file}")

            with open(path_to_xml_file, 'r') as f:
                data = f.read()

            bs_data = BeautifulSoup(data, "xml")

            # Производим поиск путей к изображениям нужных каналов
            product_organisation = bs_data.find("Product_Organisation")
            image_files = product_organisation.find_all('IMAGE_FILE')
            file_paths = [item.contents[0] for item in image_files]
            found_file_names = []
            for band in bands:
                for cur_file_path in file_paths:
                    if cur_file_path.find(F'B{band:02}_{spatial_expansion}m') != -1:
                        found_file_names.append(cur_file_path + '.jp2')

            assert len(bands) == len(
                found_file_names), 'The number of paths found and the number of channels must match!'
            assert 3 == len(
                found_file_names), 'Too many channels! The supported number of channels is 3!'

            # Извлечение дополнительных полей
            processing_level = bs_data.find("PROCESSING_LEVEL")
            if processing_level.contents is not None:
                processing_level = processing_level.contents[0]
            else:
                processing_level = None

            product_type = bs_data.find("PRODUCT_TYPE")
            if product_type.contents is not None:
                product_type = product_type.contents[0]
            else:
                product_type = None

            product_start_tine = bs_data.find("PRODUCT_START_TIME")
            if product_start_tine.contents is not None:
                product_start_tine = product_start_tine.contents[0]
            else:
                product_start_tine = None

            product_info = {
                'filename': filename,
                "found_file_names": found_file_names,
                'product_start_tine': product_start_tine,
                'product_type': product_type,
                'processing_level': processing_level
            }
            print(f'Output: {product_info}')
            return product_info

        @task(pool='image_preprocessing', pool_slots=1)
        def merge(product_info):
            import os

            context = get_current_context()
            # TODO: Продумать что еще нужно прокинуть и как потом это безопасно удалить
            #  (Можно создать папку дага и удалить ее)  или удалять по ходу на следующем этапе
            prefix_local_storage_path = context["params"]['prefix_local_storage_path']
            filename = product_info["filename"]
            print(f'Input: {product_info}')
            DockerOperator(
                image='airflow-osgeo-gdal',
                command='gdalbuildvrt -separate '
                        f"{os.path.join(prefix_local_storage_path, filename, f'stack_{filename}.vrt')} "
                        f"{os.path.join(prefix_local_storage_path, filename, product_info['found_file_names'][0])} "
                        f"{os.path.join(prefix_local_storage_path, filename, product_info['found_file_names'][1])} "
                        f"{os.path.join(prefix_local_storage_path, filename, product_info['found_file_names'][2])} ",
                network_mode='bridge',
                task_id='docker-airflow-merge-preprocess',
                # do_xcom_push=False,
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
                mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
            ).execute(context=context)
            return product_info

        @task(pool='image_preprocessing', pool_slots=1)
        def create_rgb(product_info):
            import os

            context = get_current_context()
            prefix_local_storage_path = context["params"]['prefix_local_storage_path']
            filename = product_info["filename"]
            DockerOperator(
                image='airflow-osgeo-gdal',
                command='gdal_translate -scale 0 4096 0 255 -ot Byte  '
                        f"{os.path.join(prefix_local_storage_path, filename, f'stack_{filename}.vrt')} "
                        f"{os.path.join(prefix_local_storage_path, filename, f'stack_{filename}.tif')}",
                network_mode='bridge',
                task_id='docker-airflow-create-rgb',
                # do_xcom_push=False,
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
                mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
            ).execute(context=context)
            return product_info

        @task(pool='image_preprocessing', pool_slots=1)
        def create_tiles(product_info):
            import os

            context = get_current_context()
            prefix_local_storage_path = context["params"]['prefix_local_storage_path']
            filename = product_info["filename"]
            minimum_tile_zoom = context['params']['minimum_tile_zoom']
            maximum_tile_zoom = context['params']['maximum_tile_zoom']

            DockerOperator(
                image='airflow-osgeo-gdal',
                command='gdal2tiles.py '
                        f'-z {minimum_tile_zoom}-{maximum_tile_zoom} '
                        '-w none '
                        f"{os.path.join(prefix_local_storage_path, filename, f'stack_{filename}.tif')} "
                        f"{os.path.join(prefix_local_storage_path, filename, f'tiles_{filename}')} ",

                network_mode='bridge',
                task_id='docker-airflow-create-tiles',
                # do_xcom_push=False,
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
                mounts=[Mount(source=LOCAL_DATA_DIR, target='/data', type='bind')]
            ).execute(context=context)

            return product_info

        @task
        def delete(product_info):
            # TODO: Удалить zip
            import shutil
            import os

            context = get_current_context()
            prefix_local_storage_path = context["params"]['prefix_local_storage_path']
            filename = product_info["filename"]
            shutil.rmtree(os.path.join(f'{prefix_local_storage_path}', filename))

            path_online_storage_image_file = context["ti"].xcom_pull(
                    task_ids="initializing_parameters",
                    key="path_online_storage_image_file")
            os.remove(f'{path_online_storage_image_file}')

            path_lta_storage_image_file = context["ti"].xcom_pull(
                    task_ids="initializing_parameters",
                    key="path_lta_storage_image_file")

            os.remove(f"{path_lta_storage_image_file}")

        filename = download_image(name_folder)
        filename = unzipping(filename)
        product_info = extracting_meta_information(filename)
        product_info = merge(product_info=product_info)
        product_info = create_rgb(product_info=product_info)
        product_info = create_tiles(product_info=product_info)
        # delete(product_info)

        # create_tiles(folder=create_rgb(folder=))


    list_images = scheduler_image_id()
    initializing_parameters() >> search_images() >> [wait_for_online_storage_data,
                                                     wait_for_lta_storage_data] >> list_images

    processing_images.expand(name_folder=list_images)
