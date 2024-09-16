import json

def upload_raw_data_to_json_temp_store(data,filename:str):
    with open(filename, 'w') as file:
         json.dump(data, file, indent=4)


def get_raw_data_from_json(filename:str):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data