'''
Данный модуль содержит функционал для запроса снимков с сервера Copernicus, согласно переданным параметрам
'''
from sentinelsat import SentinelAPI
import os
from sentinelsat.exceptions import SentinelAPIError
from typing import List, Dict
from utils import connect_sentinel_api
from shapely import geometry
import time
import logging.config
import pandas as pd

_log_format = "%(asctime)s\t%(levelname)s\t%(name)s\t" \
              "%(filename)s.%(funcName)s " \
              "line: %(lineno)d | \t%(message)s"
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(logging.Formatter(_log_format))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)

# Указать данные для авторизации
USER_LOGIN = os.getenv("USER_LOGIN", 'yami116')  # sej44363@omeie.com
USER_PASSWORD = os.getenv("USER_PASSWORD", '12345678qwerty')
URL_API = 'https://scihub.copernicus.eu/dhus'
NUM_RECONNECTION_ATTEMPTS = 5
NUM_QUERY_ATTEMPTS = 5
# Прокинуть уникальный id
PATH_ONLINE_STORAGE_IMAGE_FILE = os.getenv("PATH_ONLINE_STORAGE_IMAGE_FILE", None)
PATH_LTA_STORAGE_IMAGE_FILE = os.getenv("PATH_LTA_STORAGE_IMAGE_FILE", None)
DAG_ID = os.getenv("DAG_ID", None)


class MetaInformationPolygon:
    def __init__(self, polygon):
        self.polygon = polygon
        self.cloudcoverpercentage = (None, None)
        self.platformname = 'Sentinel-2'
        self.processinglevel = 'Level-2A'
        self.date = ('20230801', '20230826')


def request_unloaded_images_db(list_images: Dict) -> List:
    '''
    Функция для запроса идентификаторов незагруженных на сервер снимков

    Parameters
    ------------
    list_images: `List`
        Список идентификаторов снимков

    Returns
    ------------
    `List`
        Список идентификаторов незагруженных снимков
    '''
    return list_images


def request_polygons_db():
    """
    Функция для запроса необходимых полигонов для поиска снимков ДЗЗ

    Returns
    ---------
    `List`
        dfd
    """
    polygons = []
    poly = geometry.Polygon([[44.680385, 54.721345],
                             [46.226831, 54.781341],
                             [46.306982, 53.698870],
                             [44.392784, 53.779930]])

    example_polygon = MetaInformationPolygon(polygon=poly)
    polygons.append(example_polygon)
    result = polygons
    return result


def search_images(api: SentinelAPI,
                  polygons: List[MetaInformationPolygon],
                  num_query_attempts: int = 5) -> Dict:
    """
    Функция для запроса изображений с помощью API Copernicus

    Parameters
    ------------
    api: `SentinelAPI`
        API Copernicus
    polygons: `List[MetaInformationPolygon]`
        Список запрашиваемых полигонов с параметрами
    num_query_attempts: `int`
        Количество повторных запросов в случае неудачи

    Returns
    ------------
    `List`
        Список найденных изображений, соответствующих переданным полигонам
    """
    sleep_time = 5
    result = {"image_id": [],
              'filename': []}

    # result = []
    for polygon in polygons:
        cur_num_query_attempts = 0
        while cur_num_query_attempts < num_query_attempts:
            try:
                products = api.query(polygon.polygon,
                                     date=polygon.date,
                                     platformname=polygon.platformname,
                                     processinglevel=polygon.processinglevel,
                                     cloudcoverpercentage=polygon.cloudcoverpercentage
                                     )
                # result.append(products.keys())
                result["image_id"].extend(products.keys())
                result['filename'].extend([item['filename'] for item in products.values()])
                break
            except SentinelAPIError as exception_sentinel_api:
                cur_num_query_attempts += 1
                logger.warning(f"The request could not be executed. Exception: {exception_sentinel_api}. "
                               f"Retry after {sleep_time} seconds")
                time.sleep(sleep_time)
                sleep_time += 1
        else:
            raise SentinelAPIError("The request could not be executed!")

    return result


def saving_images_information(info_images: Dict[List, List], path_to_save: str):
    pd.DataFrame(info_images).to_csv(path_to_save, index=False)


def split_images_storage(api, image_ids: Dict):
    '''
    Функция для разделения id изображений по их местоположению LTA и online хранилище

    Parameters
    -----------
    api: `SentinelAPI`
        Класс, представляющий функционал SentinelAPI
    image_ids: `List`
        Список id изображений

    Returns
    -----------
    `List`, `List`
        Список id изображений, находящихся в online хранилище, Список id изображений, находящихся в LTA хранилище
    '''
    images_online_storage, images_lta_storages = {"image_id": [], 'filename': []}, \
        {"image_id": [], 'filename': []}
    for cur_ind in range(len(image_ids["image_id"])):
        if api.is_online(image_ids["image_id"][cur_ind]):
            images_online_storage['image_id'].append(image_ids["image_id"][cur_ind])
            images_online_storage['filename'].append(image_ids["filename"][cur_ind])
        else:
            images_lta_storages['image_id'].append(image_ids["image_id"][cur_ind])
            images_lta_storages['filename'].append(image_ids["filename"][cur_ind])

    return images_online_storage, images_lta_storages


def adding_dag_id_filename(path_to_file: str, dag_id: str):
    return os.path.splitext(path_to_file)[0] + '_' + dag_id + os.path.splitext(path_to_file)[1]


# @click.command()
# @click.option('--dag-id', type=str, default=None, help='DAG id')
def main():
    # todo: Исправить вывод логера
    # todo: Сделать переподключение когда упал инет
    logger.info(f'Environments:')
    logger.info(f'\t DAG_ID: {DAG_ID}')
    logger.info(f'\t PATH_ONLINE_STORAGE_IMAGE_FILE: {PATH_ONLINE_STORAGE_IMAGE_FILE}')
    logger.info(f'\t PATH_LTA_STORAGE_IMAGE_FILE: {PATH_LTA_STORAGE_IMAGE_FILE}')

    api = connect_sentinel_api(USER_LOGIN, USER_PASSWORD, URL_API, NUM_RECONNECTION_ATTEMPTS)
    logger.info("Authorization was successful!")

    polygons = request_polygons_db()
    logger.info(f"Polygons have been successfully obtained! Number of polygons {len(polygons)} pieces")

    found_images = search_images(api, polygons, NUM_QUERY_ATTEMPTS)
    logger.info(f"The image request was successfully completed. {len(found_images)} images found")

    unloaded_images = request_unloaded_images_db(found_images)
    logger.info(f"The request for uploaded images was successfully executed. "
                f"Number of images to upload: {len(unloaded_images)}")

    images_online_storage, images_lta_storages = split_images_storage(api, unloaded_images)
    # filename_online_storage = adding_dag_id_filename(PATH_TO_SAVE_RESULTS, DAG_ID)
    # saving_images_information(images_online_storage, filename_online_storage)
    saving_images_information(images_online_storage, PATH_ONLINE_STORAGE_IMAGE_FILE)

    # filename_lta_storage = adding_dag_id_filename(PATH_TO_SAVE_LTA_RESULTS, DAG_ID)
    # saving_images_information(images_lta_storages, filename_lta_storage)
    saving_images_information(images_lta_storages, PATH_LTA_STORAGE_IMAGE_FILE)

    # Авторизация на API Copernicus
    # Запрос необходимых полигонов на сервере приложения
    # Запрос этих полигонов на Copernicus
    # Запрос сравнения полученных полигонов и уже скаченных на сервере приложения
    # Создание csv  с результатами работы (Приписать к имени уникальный id)


if __name__ == "__main__":
    main()
