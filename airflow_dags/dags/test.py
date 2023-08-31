import pandas as pd

if __name__ ==  "__main__":
    data = pd.read_csv('./../data/images_online_storage_74709328-bdfb-4cbf-9109-a1071141d7c4.csv')
    print(f'Data !!!!!!!!!!!!!: {len(data)}')
    result = []
    for ind, row in data.iterrows():
        # result.append({'image_id': row["image_id"],
        #                'filename': row['filename']})
        result.append(row["image_id"])
    print(result)