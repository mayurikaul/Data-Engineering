import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from alpha_vantage_utils import get_alpha_vantage_raw_data
from json_utils import upload_raw_data_to_json_temp_store, get_raw_data_from_json
from aws_utils import upload_to_aws_s3, get_data_from_s3
from sql_utils import connect_to_db, export_data_to_sql
from timeseries_utils import process_raw_time_series_data


#Creating the time series table
def create_time_series_table(sql_database:str,table_name:str):
    engine = connect_to_db(sql_database)
    try:
        connection = engine.connect()
        print("Connection successful!")
        connection.close()  
    except Exception as e:
        print(f"Error connecting to the database: {e}")

    Base = declarative_base()

    class TimeSeries(Base):
        __tablename__ = table_name

        id = Column(Integer, primary_key=True, autoincrement=True)
        date = Column(DateTime, nullable=False)
        open = Column(Float, nullable=False)
        high = Column(Float, nullable=False)
        low = Column(Float, nullable=False)
        close = Column(Float, nullable=False)
        volume = Column(Integer, nullable=False)
        ticker = Column(String(5), nullable=False)
        inserted_at = Column(DateTime, nullable=False)
    
    Base.metadata.create_all(engine)



#This function sets up an initial data - pulls from AV, stores in AWS, gets from AWS, stores in SQL main table. 
def single_job(ticker:str):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey=AFJ71MOJKQYF8WN4'
    data_av = get_alpha_vantage_raw_data(url)
    upload_raw_data_to_json_temp_store(data_av,f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}timeseries.json')
    data_j = get_raw_data_from_json(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}timeseries.json')
    df = process_raw_time_series_data(data_j)
    upload_to_aws_s3(df, 'time-seriesdata', f'{ticker}_Time_Series.csv')
    df = get_data_from_s3('time-seriesdata', f'{ticker}_Time_Series.csv')
    export_data_to_sql('timeseries', df, f'{ticker.lower()}_timeseries')



# To create an initial (empty)changes log to track changes:
# We do not run this every time, since it would create a new changes log, deleting the previous entries. 
# We run this once when setting up the data for a ticker. 

def create_changes_log(ticker:str):
    columns = ['action_type', 'new_id', 'old_id', 'new_date', 'old_date', 'new_open', 'old_open',
                'new_high', 'old_high', 'new_low', 'old_low', 'new_close', 'old_close', 'new_volume',
                   'old_volume', 'new_inserted_at',  'old_inserted_at']

    changes_log = pd.DataFrame(columns=columns)
    changes_log.to_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_timeseries_changes_log.csv', index = False)
