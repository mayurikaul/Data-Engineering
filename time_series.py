import requests
import pandas as pd
import json
import os
from io import StringIO
import boto3
import functions
from sqlalchemy import create_engine

#This code is for one ticker AAPL

#For one ticker: AAPL
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey=AFJ71MOJKQYF8WN4"

def process_raw_time_series_data(data):
    time_series = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(time_series, orient = 'index') 
    df.index = pd.to_datetime(df.index)
    df = df.astype(float) 
    df["Ticker"] = "AAPL"
    return df


def single_job():
    data_av = functions.get_alpha_vantage_raw_data(url)
    functions.upload_raw_data_to_json_temp_store(data_av, 'AAPLtimeseries.json')
    data_j = functions.get_raw_data_from_json('AAPLtimeseries.json')
    df = process_raw_time_series_data(data_j)
    functions.upload_to_aws_s3(df, 'time-seriesdata', 'AAPL_Time_Series.csv')
    df = functions.get_data_from_s3('time-seriesdata', 'AAPL_Time_Series.csv')
    functions.import_data_to_sql('timeseries', df, 'aapl_timeseries')


single_job()



#Investigate the following(Watch vid)
#from dataclasses import dataclass
#     #arjun codes has a vid on dataclasses + functional style.
#     @dataclass
#     class AlphaVantageNewsJob:
#         start_date: datetime
#         end_date: datetime
#         ticker: str
#     
#       job_config = AlphaVantageNewsJob(start_date=start_date, end_date=end_date, ticker=ticker)


#For file names (random number generator): import uuid 