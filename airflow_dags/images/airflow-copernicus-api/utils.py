from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from sentinelsat.exceptions import ServerError, UnauthorizedError, SentinelAPIError


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
        Класс, представляющий функционал API Copernicus
    '''
    cur_num_reconnection_attempts = 0
    while cur_num_reconnection_attempts < num_reconnection_attempts:
        try:
            api = SentinelAPI(user_login, user_password, url_api)
            return api
        except (ServerError, UnauthorizedError) as authorization_exception:
            print(f"Exception: {authorization_exception}")

        cur_num_reconnection_attempts += 1
    raise SentinelAPIError("Failed to log in to the server")
