import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

#Scripts to import
from alpha_vantage_utils import get_alpha_vantage_raw_data
from json_utils import upload_raw_data_to_json_temp_store, get_raw_data_from_json
from aws_utils import upload_to_aws_s3, get_data_from_s3
from timeseries_utils import process_raw_time_series_data, create_temp_and_merge_timeseries, log_timeseries_changes


def update_job(ticker:str):
    #url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey=AFJ71MOJKQYF8WN4'
    #data_av = get_alpha_vantage_raw_data(url)
    #upload_raw_data_to_json_temp_store(data_av, f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}timeseries_temp.json')
    data_j = get_raw_data_from_json(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}timeseries_temp.json')
    df = process_raw_time_series_data(data_j)
    upload_to_aws_s3(df, 'time-seriesdata', f'{ticker}_Time_Series_temp.csv')
    df = get_data_from_s3('time-seriesdata', f'{ticker}_Time_Series_temp.csv')
    output_log = create_temp_and_merge_timeseries(ticker,'timeseries', df)
    log_timeseries_changes(output_log, ticker)


update_job('AAPL')