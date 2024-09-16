import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, text
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from alpha_vantage_utils import get_alpha_vantage_raw_data
from json_utils import upload_raw_data_to_json_temp_store, get_raw_data_from_json
from aws_utils import upload_to_aws_s3, get_data_from_s3
from sql_utils import connect_to_db, export_data_to_sql
from earnings_utils import process_earnings


def create_yearly_earnings_table(sql_database:str, table_name:str):
    engine = connect_to_db(sql_database)
    
    Base = declarative_base()

    class YearlyEarnings(Base):
        __tablename__ = table_name

        id = Column(Integer, primary_key=True, autoincrement=True)
        fiscal_date_ending = Column(DateTime, nullable=False)
        reported_eps = Column(Float, nullable=False)
        ticker = Column(String(5), nullable=False)
        inserted_at = Column(DateTime, nullable=False)
    
    Base.metadata.create_all(engine)

    
def create_quarterly_earnings_table(sql_database:str,table_name:str):
    engine = connect_to_db(sql_database)
    
    Base = declarative_base()

    class QuarterlyEarnings(Base):
        __tablename__ = table_name

        id = Column(Integer, primary_key=True, autoincrement=True)
        fiscal_date_ending = Column(DateTime, nullable=False)
        reported_date = Column(DateTime, nullable=False)
        reported_eps = Column(Float, nullable=False)
        estimated_eps = Column(Float)
        surprise = Column(Float)
        surprise_perc = Column(Float)
        report_time = Column(String)
        ticker = Column(String(5), nullable=False)
        inserted_at = Column(DateTime, nullable=False)
    
    Base.metadata.create_all(engine)
    

# create_yearly_earnings_table('earnings','aapl_yearly')
# create_quarterly_earnings_table('earnings','aapl_quarterly')


def single_job(ticker:str):
    url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey=AFJ71MOJKQYF8WN4'
    data_av = get_alpha_vantage_raw_data(url)
    upload_raw_data_to_json_temp_store(data_av, f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_earnings.json')
    data_j = get_raw_data_from_json(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_earnings.json')
    df_yearly, df_quarterly = process_earnings(data_j)
    upload_to_aws_s3(df_yearly, "earnings-dataset", f"Yearly/{ticker}_Earnings.csv")
    upload_to_aws_s3(df_quarterly, "earnings-dataset", f"Quarterly/{ticker}_Earnings.csv")
    df_quarterly = get_data_from_s3("earnings-dataset", f"Quarterly/{ticker}_Earnings.csv")
    df_yearly = get_data_from_s3("earnings-dataset", f"Yearly/{ticker}_Earnings.csv")
    export_data_to_sql('earnings',df_quarterly, f'{ticker.lower()}_quarterly')
    export_data_to_sql('earnings',df_yearly, f'{ticker.lower()}_yearly')

#single_job('AAPL')

#Create a changes log:

def create_yearly_changes_log(ticker:str):
    columns_yearly = ['action_type', 'new_id', 'old_id', 'new_fiscal_date_ending',
                  'old_fiscal_date_ending', 'new_reported_eps', 'old_reported_eps',
                   'new_inserted_at', 'old_inserted_at']
    
    yearly_changes_log = pd.DataFrame(columns=columns_yearly)
    yearly_changes_log.to_csv(f"{ticker}_yearly_earnings_changes_log.csv", index = False)


def create_quarterly_changes_log(ticker:str):
    columns_quarterly = ['action_type', 'new_id', 'old_id', 'new_fiscal_date_ending', 'old_fiscal_date_ending', 'new_reported_date', 
'old_reported_date', 'new_reported_eps', 'old_reported_eps', 'new_estimated_eps', 'old_estimated_eps',
'new_surprise', 'old_surprise', 'new_surprise_perc', 'old_surprise_perc', 'new_report_time', 'old_report_time',
'new_inserted_at', 'old_inserted_at']
    
    quarterly_changes_log = pd.DataFrame(columns=columns_quarterly)
    quarterly_changes_log.to_csv(f"{ticker}_quarterly_earnings_changes_log.csv", index = False)
