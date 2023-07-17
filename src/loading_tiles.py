'''
Модуль производит загрузку сгенерированных tiles на сервер карт https://github.com/betepok506/map_api
'''
import os
import click
import base64
import json
import requests
import io
from PIL import Image

SERVER_URI = os.getenv("SERVER_URI", 'http://localhost:8001/add_tiles/')
MAX_LOADING = 50


def send_request(array_tiles):
    payload = json.dumps({"tiles": array_tiles})

    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(SERVER_URI,
                             data=payload,
                             # files={'upload_file': open(os.path.join(path_to_tiles, zoom, y, x), 'rb')},
                             headers=headers)
    try:
        data = response.json()
        print(data)
    except requests.exceptions.RequestException:
        print(response.text)


# @click.command()
# @click.option("--path_to_tiles", help="Path to the tile folder")
def loading(path_to_tiles: str):
    if not os.path.exists(path_to_tiles):
        raise FileNotFoundError("The tile catalog was not found!")

    message = []
    for zoom in os.listdir(path_to_tiles):
        if os.path.isdir(os.path.join(path_to_tiles, zoom)):
            for x in os.listdir(os.path.join(path_to_tiles, zoom)):
                for y in os.listdir(os.path.join(path_to_tiles, zoom, x)):

                    img = Image.open(os.path.join(path_to_tiles, zoom, x, y), mode='r')
                    img_byte_arr = io.BytesIO()
                    img.save(img_byte_arr, format='PNG')
                    img_byte_arr = img_byte_arr.getvalue()
                    im_b64 = base64.b64encode(img_byte_arr).decode("utf8")
                    message.append({
                        "map_name": "landsat8",
                        'x': int(x),
                        'y': int(y.split('.')[0]),
                        'z': int(zoom),
                        "image": im_b64,
                    })

                    if len(message) >= MAX_LOADING:
                        send_request(message)
                        message.clear()

    if len(message):
        send_request(message)


if __name__ == "__main__":
    loading("D:\\diploma_project\\aerial_photo_processing_service\\data\\tilesdir_28")
