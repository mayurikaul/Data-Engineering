import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

#Scripts to import
from alpha_vantage_utils import get_alpha_vantage_raw_data
from json_utils import upload_raw_data_to_json_temp_store, get_raw_data_from_json
from aws_utils import upload_to_aws_s3, get_data_from_s3
from earnings_utils import (process_earnings, create_temp_and_merge_yearly, 
                            create_temp_and_merge_quarterly, log_yearly_changes, log_quarterly_changes)



def update_job(ticker:str):
    url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey=AFJ71MOJKQYF8WN4'
    data_av = get_alpha_vantage_raw_data(url)
    upload_raw_data_to_json_temp_store(data_av, f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_earnings_temp.json')
    data_j = get_raw_data_from_json(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_earnings_temp.json')
    df_yearly, df_quarterly = process_earnings(data_j)
    upload_to_aws_s3(df_yearly, "earnings-dataset", f"Yearly/{ticker}_Earnings_temp.csv")
    upload_to_aws_s3(df_quarterly, "earnings-dataset", f"Quarterly/{ticker}_Earnings_temp.csv")
    df_quarterly = get_data_from_s3("earnings-dataset", f"Quarterly/{ticker}_Earnings_temp.csv")
    df_yearly = get_data_from_s3("earnings-dataset", f"Yearly/{ticker}_Earnings_temp.csv")
    yearly_output_log = create_temp_and_merge_yearly(ticker, 'earnings', df_yearly)
    quarterly_output_log = create_temp_and_merge_quarterly(ticker, 'earnings', df_quarterly)
    log_yearly_changes(yearly_output_log, ticker)
    log_quarterly_changes(quarterly_output_log, ticker)


update_job('AAPL')