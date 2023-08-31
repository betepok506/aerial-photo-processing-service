# Описание

# Запуск
Перед запуском контейнера airflow необходимо экспортировать переменные окружения из корневого каталога: 
* Linux:
 ```
export LOCAL_DATA_DIR=$(pwd)/data
export PASSWORD=YOUR_PASSWORD
export LOCAL_MLRUNS_DIR=$(pwd)/mlruns
export FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")
 ```
* Windows
 ```
$env:PASSWORD=YOUR_PASSWORD
$env:LOCAL_DATA_DIR="$(pwd)/data"
$env:AIRFLOW_PROJ_DIR="$(pwd)" // Определиться с монтированием папки data
$env:LOCAL_MLRUNS_DIR="$(pwd)/mlruns"
$env:FERNET_KEY="$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")"
 ```

Далее необходимо выполнить:
```commandline
cd airflow_dags
```

Для запуска контейнера необходимо выполнить следующую команду:
```commandline
docker-compose up --build
```

Для очистки контейнеров и volumes необходимо использовать следующую команду:
```commandline
docker-compose down --volumes 
```

Данные для авторизации по умолчанию:
```commandline
login: airflow
password: airflow
```


# Скоро удалю:

Разобраться как работает и завернуть все в докер

https://docs.mapbox.com/help/tutorials/processing-satellite-imagery/

rio stack --rgb LC08_L1TP_160043_20181207_20181211_01_T1/LC08_L1TP_160043_20181207_20181211_01_T1_B{3,2,1}.TIF landsat8_stack.tif

rio warp --resampling bilinear landsat8_stack.tif landsat8_mercator.tif
Флаг --dst-crs EPSG:3857 не работает()

rio color --co photometric=rgb --out-dtype uint8 landsat8_mercator.tif landsat8_color.tif sigmoidal rgb 35 0.17

# GDAL Landsat

Смержить каналы:

docker run -v D:/diploma_project/aerial_photo_processing_service/data:/data osgeo/gdal gdal_merge.py -o 
/data/output_131016_20210729_20210804.tif 
/data/LC08_L2SP_131016_20210729_20210804_02_T1/LC08_L2SP_131016_20210729_20210804_02_T1_SR_B7.TIF 
/data/LC08_L2SP_131016_20210729_20210804_02_T1/LC08_L2SP_131016_20210729_20210804_02_T1_SR_B6.TIF 
/data/LC08_L2SP_131016_20210729_20210804_02_T1/LC08_L2SP_131016_20210729_20210804_02_T1_SR_B2.TIF  -separate

Сделать rgb:

docker run -v D:/diploma_project/aerial_photo_processing_service/data:/data osgeo/gdal gdal_translate -scale -ot Byte /data/output_131016_20210729_20210804.tif  /data/output_8bit_131016_20210729_20210804.tif 

Сгенерировать tiles:
 Старый вариант 
docker run --rm -v D:/diploma_project/aerial_photo_processing_service/data:/data niene/gdal2tiles-leaflet -l -p raster -z 0-5 -w none /data/output_8bit.tif /data/tilesdir

Новый и правильный 
 docker run --rm -v D:/diploma_project/aerial_photo_processing_service/data:/data osgeo/gdal gdal2tiles.py -z 7-12 -w none /data/output_8bit_131016_20210729_20210804.tif  /data/tilesdir_131016_20210729_20210804


# GDAL Sentinel

merge
docker run -v D:/diploma_project/aerial_photo_processing_service/data:/data osgeo/gdal 
gdalbuildvrt -separate 
/data/stack.vrt 
/data/T38UNE_20230825T080611_B02_10m.jp2 
/data/T38UNE_20230825T080611_B03_10m.jp2 
/data/T38UNE_20230825T080611_B04_10m.jp2

Convert
 docker run -v D:/diploma_project/aerial_photo_processing_service/data:/data osgeo/gdal gdal_translate 
 -scale 0 4096 0 255 -ot Byte /data/stack.vrt /data/stack.tif

tiles
docker run -v D:/diploma_project/aerial_photo_processing_service/data:/data osgeo/gdal gdal2tiles.py 
-z 7-12 /data/stack.tif /data/test_san

# Улучшения:
- [ ] Синхронизация Dags c GitHub
- [ ] Добавить поддержку скачивания снимков с LTA
- [ ] Настроить удаленный запуск Dags
- [ ] Настроить Dag по закачке снимков с параметрами
- [ ] Добавить скрипт для выкачки снимков с API Sentinel