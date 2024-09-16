from io import StringIO
import boto3
import pandas as pd

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