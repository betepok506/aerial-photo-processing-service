import time
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from sentinelsat.exceptions import ServerError, UnauthorizedError, SentinelAPIError
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


def connect_sentinel_api(user_login: str,
                         user_password: str,
                         url_api: str,
                         num_reconnection_attempts: int) -> SentinelAPI:
    '''
    Функция для авторизации на сервере Copernicus

    Parameters
    ------------
    user_login: `str`
        Логин для авторизации на сервере Copernicus
    user_password: `str`
        Пароль для авторизации на сервере Copernicus
    url_api: `str`
        URL адрес API сервера Copernicus
    num_reconnection_attempts: `str`
        Количество попыток в случае неудачной авторизации

    Returns
    ------------
    `SentinelAPI`
        Класс, представляющий функционал SentinelAPI
    '''
    cur_num_reconnection_attempts = 0
    sleep_time = 5
    while cur_num_reconnection_attempts < num_reconnection_attempts:
        try:
            api = SentinelAPI(user_login, user_password, url_api)
            return api
        except (ServerError, UnauthorizedError) as authorization_exception:
            logger.warning(f"Authorization failed. Exception: {authorization_exception}. "
                           f"Retry after {sleep_time} seconds.")
            time.sleep(sleep_time)
            sleep_time += 5

        cur_num_reconnection_attempts += 1
    raise SentinelAPIError("Failed to log in to the server")
