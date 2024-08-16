import requests
import pandas as pd
import json
import os
from io import StringIO
import boto3


def get_alpha_vantage_raw_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error ocurred: {e}")
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
    

def upload_raw_data_to_json_temp_store(data,filename:str):
    with open(filename, 'w') as file:
         json.dump(data, file, indent=4)


def get_raw_data_from_json(filename:str):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data


def upload_to_aws_s3(df, bucket_name:str, filename:str):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3 = boto3.client('s3')
    bucket_name = bucket_name
    s3_file_name = filename
    s3.put_object(Bucket=bucket_name, Key=s3_file_name, Body=csv_buffer.getvalue())