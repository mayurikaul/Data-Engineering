import requests
import pandas as pd
import json
import os
from io import StringIO
import boto3
from sqlalchemy import create_engine
import pyodbc


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


def upload_to_aws_s3(df, bucketname:str, filename:str):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3 = boto3.client('s3')
    bucket_name = bucketname
    s3_file_name = filename
    s3.put_object(Bucket=bucket_name, Key=s3_file_name, Body=csv_buffer.getvalue())


def get_data_from_s3(bucketname:str, filename:str):
    s3_client = boto3.client('s3')
    bucket_name = bucketname
    object_key = filename
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(content))
    return df


# def clean_column_names(columns):
#     cleaned_columns = []
#     for col in columns:
#         col = col.strip()
#         col = col.replace(' ', '_').replace('.','_')
#         col = '_'.join(filter(None, col.split('_')))
#         if col[0].isdigit():
#             col = 'col' + col
#         cleaned_columns.append(col)
#     return cleaned_columns


def import_data_to_sql(sql_database:str, df, sql_table:str):
    username = 'sa'
    password = 'OldRectory1'
    #host = 'localhost' 
    server = 'localhost'
    driver = 'ODBC Driver 17 for SQL Server'
    #port = '1433' 
    #connection_string = f'mysql+pymysql://{username}:{password}@{host}:{port}/{sql_database}'
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{sql_database}?driver={driver}"
    engine = create_engine(connection_string)
    try:
        connection = engine.connect()
        print("Connection successful!")
        connection.close()  
    except Exception as e:
        print(f"Error connecting to the database: {e}")

    df.to_sql(sql_table, con=engine, if_exists='append', index=False)



