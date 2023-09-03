from bs4 import BeautifulSoup
import glob

if __name__ ==  "__main__":
    xml_search_result = glob.glob('MTD*')
    assert len(xml_search_result) != 0, "The file with the information was not found!"
    PATH_TO_XML_FILE = xml_search_result[0]

    # PATH_TO_XML_FILE = 'MTD_MSIL2A.xml' # Путь до файла с информацией
    # Найти этот файл через glob

    # Каналы для извлечения
    BANDS = [2, 3, 4]
    # Пространственное расширение
    SPATIAL_EXPANSION = 10
    with open(PATH_TO_XML_FILE, 'r') as f:
        data = f.read()

    Bs_data = BeautifulSoup(data, "xml")

    # Производим поиск путей к изображениям нужных каналов
    product_organisation = Bs_data.find("Product_Organisation")
    image_files = product_organisation.find_all('IMAGE_FILE')
    file_paths = [item.contents[0] for item in image_files]
    found_file_names = []
    for band in BANDS:
        for cur_file_path in file_paths:
            if cur_file_path.find(F'B{band:02}_10m') != -1:
                found_file_names.append(cur_file_path)

    assert len(BANDS) == len(found_file_names), 'The number of paths found and the number of channels must match!'

    # Извлечение дополнительных полей

    processing_level = Bs_data.find("PROCESSING_LEVEL")
    if processing_level.contents is not None:
        processing_level = processing_level.contents[0]
    else:
        processing_level = None

    product_type = Bs_data.find("PRODUCT_TYPE")
    if product_type.contents is not None:
        product_type = product_type.contents[0]
    else:
        product_type = None

    product_start_tine = Bs_data.find("PRODUCT_START_TIME")
    if product_start_tine.contents is not None:
        product_start_tine = product_start_tine.contents[0]
    else:
        product_start_tine = None

    print(found_file_names)
