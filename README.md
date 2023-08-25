Разобраться как работает и завернуть все в докер

https://docs.mapbox.com/help/tutorials/processing-satellite-imagery/

rio stack --rgb LC08_L1TP_160043_20181207_20181211_01_T1/LC08_L1TP_160043_20181207_20181211_01_T1_B{3,2,1}.TIF landsat8_stack.tif

rio warp --resampling bilinear landsat8_stack.tif landsat8_mercator.tif
Флаг --dst-crs EPSG:3857 не работает()

rio color --co photometric=rgb --out-dtype uint8 landsat8_mercator.tif landsat8_color.tif sigmoidal rgb 35 0.17

# GDAL

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


Улучшения:
- [ ] Синхронизация Dags c GitHub
- [ ] Настроить удаленный запуск Dags
- [ ] Настроить Dag по закачке снимков с параметрами
- [ ] Добавить скрипт для выкачки снимков с API Sentinel