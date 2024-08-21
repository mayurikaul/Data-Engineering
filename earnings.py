import requests
import pandas as pd
import json
import os
from io import StringIO
import boto3
import functions




# #Earnings data
# raw_data = pd.DataFrame()
# for i in range(len(test_ticker)):
#     url = "https://www.alphavantage.co/query?function=EARNINGS&symbol={0}&apikey=AFJ71MOJKQYF8WN4".format(test_ticker[i])
#     response = requests.get(url)
#     if response.status_code == 200:
#         data = response.json()
#         #Fill in with the data you want from earnings - annual and quarterly?
#     else: print("Failed to retrieve data from API. Status Code: ", response.status_code)


#For a single ticker:

url = "https://www.alphavantage.co/query?function=EARNINGS&symbol=AAPL&apikey=AFJ71MOJKQYF8WN4"

def process_earnings(data):
    yearly = data["annualEarnings"]
    df_yearly = pd.DataFrame.from_dict(yearly) 
    df_yearly["Ticker"] = "AAPL"
    quarterly = data["quarterlyEarnings"]
    df_quarterly = pd.DataFrame.from_dict(quarterly)
    df_quarterly["Ticker"] = "AAPL"
    return df_yearly,df_quarterly


def single_job():
    data_av = functions.get_alpha_vantage_raw_data(url)
    functions.upload_raw_data_to_json_temp_store(data_av, 'AAPL_earnings.json')
    data_j = functions.get_raw_data_from_json("AAPL_earnings.json")
    df_yearly, df_quarterly = process_earnings(data_j)
    functions.upload_to_aws_s3(df_yearly, "earnings-dataset", "Yearly/AAPL_Earnings.csv")
    functions.upload_to_aws_s3(df_quarterly, "earnings-dataset", "Quarterly/AAPL_Earnings.csv")
    df_quarterly = functions.get_data_from_s3("earnings-dataset", "Quarterly/AAPL_Earnings.csv")
    df_yearly = functions.get_data_from_s3("earnings-dataset", "Yearly/AAPL_Earnings.csv")
    functions.import_data_to_sql('earnings', df_quarterly, 'aapl_quarterly')
    functions.import_data_to_sql('earnings', df_yearly, 'aapl_yearly')


single_job()
