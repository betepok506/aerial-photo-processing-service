'''
Данный модуль содержит функционал для запроса снимков с сервера Copernicus, согласно переданным параметрам
'''
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import os
from sentinelsat.exceptions import SentinelAPIError
from typing import List
from utils import connect_sentinel_api
from shapely import geometry
import time
import logging
import logging.config

_log_format = "%(asctime)s\t%(levelname)s\t%(name)s\t" \
              "%(filename)s.%(funcName)s " \
              "line: %(lineno)d | \t%(message)s"
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(logging.Formatter(_log_format))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)

USER_LOGIN = os.getenv("USER_LOGIN", 'yami116')  # sej44363@omeie.com
USER_PASSWORD = os.getenv("USER_PASSWORD", '12345678qwerty')
URL_API = 'https://scihub.copernicus.eu/dhus'
NUM_RECONNECTION_ATTEMPTS = 5
NUM_QUERY_ATTEMPTS = 5


class MetaInformationPolygon:
    def __init__(self, polygon):
        self.polygon = polygon
        self.cloudcoverpercentage = (None, None)
        self.platformname = 'Sentinel-2'
        self.processinglevel = 'Level-2A'
        self.date = ('20190601', '20190626')


def request_unloaded_images_db(list_images: List) -> List:
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
                  num_query_attempts: int = 5) -> List:
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
    result = []
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
                result.extend(products)
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


def saving_images_information(info_images):
    print(info_images)
    pass


def main():
    api = connect_sentinel_api(USER_LOGIN, USER_PASSWORD, URL_API, NUM_RECONNECTION_ATTEMPTS)
    logger.info("Authorization was successful!")

    polygons = request_polygons_db()
    logger.info(f"Polygons have been successfully obtained! Number of polygons {len(polygons)} pieces")

    found_images = search_images(api, polygons, NUM_QUERY_ATTEMPTS)
    logger.info(f"The image request was successfully completed. {len(found_images)} images found")

    unloaded_images = request_unloaded_images_db(found_images)
    logger.info(f"The request for uploaded images was successfully executed. "
                f"Number of images to upload: {len(unloaded_images)}")
    saving_images_information(unloaded_images)

    # Авторизация на API Copernicus
    # Запрос необходимых полигонов на сервере приложения
    # Запрос этих полигонов на Copernicus
    # Запрос сравнения полученных полигонов и уже скаченных на сервере приложения
    # Создание json  с результатами работы (Приписать к имени уникальный id)


if __name__ == "__main__":
    main()
