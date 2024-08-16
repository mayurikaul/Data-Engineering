import requests
import pandas as pd
import json
from io import StringIO
import boto3
import functions


# News data 1 ticker
# So either limit=50 or 1000
# Plan: for each month from March 2022 to present we do a request for news_stories with a limit of 50
# For now we will only look at the last month due to request issues

url = "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&limit=50&apikey=AFJ71MOJKQYF8WN4"

def process_raw_news_data(data):
    news = data["feed"]
    df_news = pd.DataFrame.from_dict(news)
    df_news['Ticker'] = 'AAPL'
    return df_news


def single_job():
    data_av = functions.get_alpha_vantage_raw_data(url)
    functions.upload_raw_data_to_json_temp_store(data_av, "AAPL_news.json")
    data_j = functions.get_raw_data_from_json("AAPL_news.json")
    df_news = process_raw_news_data(data_j)
    functions.upload_to_aws_s3(df_news, 'news-dataset', 'AAPL-news.csv')


single_job()

