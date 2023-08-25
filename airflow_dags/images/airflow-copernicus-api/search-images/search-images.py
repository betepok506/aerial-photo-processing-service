'''
Данный модуль содержит функционал для запроса снимков с сервера Copernicus, согласно переданным параметрам
'''
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import os
from sentinelsat.exceptions import SentinelAPIError
from typing import List
from utils import connect_sentinel_api

USER_LOGIN = os.getenv("USER_LOGIN", 'yami116')  # sej44363@omeie.com
USER_PASSWORD = os.getenv("USER_PASSWORD", '12345678qwerty')
URL_API = 'https://scihub.copernicus.eu/dhus'
NUM_RECONNECTION_ATTEMPTS = 5
NUM_QUERY_ATTEMPTS = 5


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


def request_polygons_db(polygons):
    """
    Функция для запроса необходимых полигонов для поиска снимков ДЗЗ

    Returns
    ---------
    `List`
        dfd
    """
    result = polygons
    return result


class MetaInformationPolygon:
    def __init__(self):
        polygon = ''
        cloudcoverpercentage = (None, None)
        platformname = 'Sentinel-2'
        processinglevel = 'Level-2A'
        date = ('20190601', '20190626')


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
                continue
            except SentinelAPIError as exception_sentinel_api:
                cur_num_query_attempts += 1
                print(f"Exception: {exception_sentinel_api}")

    return result


def saving_images_information(info_images):
    pass


def main():
    api = connect_sentinel_api(USER_LOGIN, USER_PASSWORD, URL_API, NUM_RECONNECTION_ATTEMPTS)
    polygons = request_polygons_db()
    found_images = search_images(api, polygons, NUM_QUERY_ATTEMPTS)
    unloaded_images = request_unloaded_images_db(found_images)
    saving_images_information(unloaded_images)

    # Авторизация на API Copernicus
    # Запрос необходимых полигонов на сервере приложения
    # Запрос этих полигонов на Copernicus
    # Запрос сравнения полученных полигонов и уже скаченных на сервере приложения
    # Создание json  с результатами работы (Приписать к имени уникальный id)


if __name__ == "__main__":
    main()
