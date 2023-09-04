"""
Данный модуль предназначен для загрузки изображений с Copernicus
"""
import os
import click
from sentinelsat.exceptions import SentinelAPIError, LTATriggered
from utils import connect_sentinel_api
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
PATH_TO_SAVE = os.getenv("PATH_TO_SAVE", './data')
URL_API = 'https://scihub.copernicus.eu/dhus'
NUM_RECONNECTION_ATTEMPTS = 5


# @click.command()
# @click.option('--image-id', type=str,
#               default=None,
#               help='A unique image identifier obtained from the SentinelAPI')
def main():
    image_id = os.getenv("IMAGE_ID")
    logger.info(f"Image id {image_id}")
    if image_id is None:
        raise "Image id must be a string!"

    api = connect_sentinel_api(USER_LOGIN, USER_PASSWORD, URL_API, NUM_RECONNECTION_ATTEMPTS)
    logger.info("Authorization was successful!")
    is_online = api.is_online(image_id)
    if not is_online:
        raise LTATriggered("The image could not be uploaded because it is in a long-term archive!")

    api.download(image_id, directory_path=PATH_TO_SAVE)
    logger.info(f"The download is complete!")


if __name__ == "__main__":
    main()
