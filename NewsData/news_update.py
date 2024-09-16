import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

#Scripts to import
from alpha_vantage_utils import get_alpha_vantage_raw_data
from json_utils import upload_raw_data_to_json_temp_store, get_raw_data_from_json
from aws_utils import upload_to_aws_s3, get_data_from_s3
from news_utils import process_raw_news_data, create_temp_and_merge_news, log_news_changes


def update_job(ticker:str):
    #url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&limit=50&apikey=AFJ71MOJKQYF8WN4"
    #data_av = get_alpha_vantage_raw_data(url)
    #upload_raw_data_to_json_temp_store(data_av, f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_news_temp.json')
    data_j = get_raw_data_from_json(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_news_temp.json')
    df_news = process_raw_news_data(data_j)
    upload_to_aws_s3(df_news, 'news-dataset', f'{ticker}_news_temp.csv')
    df_news = get_data_from_s3('news-dataset', f'{ticker}_news_temp.csv')
    output_log = create_temp_and_merge_news(ticker, 'news_data', df_news)
    log_news_changes(output_log, ticker)
    


update_job('AAPL')