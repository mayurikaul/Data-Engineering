import requests
import pandas as pd
import json
import os
from io import StringIO
import boto3
import functions
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone

#This code is for one ticker AAPL

#For one ticker: AAPL
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey=AFJ71MOJKQYF8WN4"

def process_raw_time_series_data(data):
    time_series = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(time_series, orient = 'index')
    df = df.reset_index()
    df.rename(columns={'index':'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df.rename(columns={'1. open' : 'open', '2. high' : 'high', '3. low' : 'low', '4. close' : 'close', '5. volume' : 'volume'}, inplace=True) 
    df['Ticker'] = "AAPL"
    df['inserted_at'] = pd.to_datetime(datetime.now(timezone.utc))
    df['inserted_at'] = df['inserted_at'].apply(lambda x: x.replace(microsecond=0))
    df['inserted_at'] = df['inserted_at'].dt.tz_localize(None)
    return df


def import_time_series_to_sql(sql_database:str, df, sql_table:str):
    username = 'sa'
    password = 'OldRectory1'
    server = 'localhost'
    driver = 'ODBC Driver 17 for SQL Server'
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{sql_database}?driver={driver}"
    engine = create_engine(connection_string)
    try:
        connection = engine.connect()
        print("Connection successful!")
        connection.close()  
    except Exception as e:
        print(f"Error connecting to the database: {e}")

    Base = declarative_base()

    class AAPLTimeSeries(Base):
        __tablename__ = 'aapl_timeseries'

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

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    df.to_sql(sql_table, con=engine, index=False, if_exists='append')



def import_time_series_to_temp_sql(sql_database:str, df, sql_table:str):
    username = 'sa'
    password = 'OldRectory1'
    server = 'localhost'
    driver = 'ODBC Driver 17 for SQL Server'
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{sql_database}?driver={driver}"
    engine = create_engine(connection_string)
    try:
        connection = engine.connect()
        print("Connection successful!")
        connection.close()  
    except Exception as e:
        print(f"Error connecting to the database: {e}")

    Base = declarative_base()

    class AAPLTimeSeries(Base):
        __tablename__ = 'aapl_timeseries_temp'

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

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    df.to_sql(sql_table, con=engine, index=False, if_exists='append')


def single_job():
    #data_av = functions.get_alpha_vantage_raw_data(url)
    #functions.upload_raw_data_to_json_temp_store(data_av, 'AAPLtimeseries.json')
    data_j = functions.get_raw_data_from_json('AAPLtimeseries.json')
    df = process_raw_time_series_data(data_j)
    functions.upload_to_aws_s3(df, 'time-seriesdata', 'AAPL_Time_Series.csv')
    df = functions.get_data_from_s3('time-seriesdata', 'AAPL_Time_Series.csv')
    import_time_series_to_sql('timeseries', df, 'aapl_timeseries')


#single_job()


def update_job():
    data_av = functions.get_alpha_vantage_raw_data(url)
    functions.upload_raw_data_to_json_temp_store(data_av, 'AAPLtimeseries_temp.json')
    data_j = functions.get_raw_data_from_json('AAPLtimeseries_temp.json')
    df = process_raw_time_series_data(data_j)
    functions.upload_to_aws_s3(df, 'time-seriesdata', 'AAPL_Time_Series_temp.csv')
    df = functions.get_data_from_s3('time-seriesdata', 'AAPL_Time_Series_temp.csv')
    import_time_series_to_temp_sql('timeseries', df, 'aapl_timeseries_temp')


#update_job()

def merge_tables(sql_database:str):
    username = 'sa'
    password = 'OldRectory1'
    server = 'localhost'
    driver = 'ODBC Driver 17 for SQL Server'
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{sql_database}?driver={driver}"
    engine = create_engine(connection_string)
    try:
        connection = engine.connect()
        print("Connection successful!")
        connection.close()  
    except Exception as e:
        print(f"Error connecting to the database: {e}")

    merge_sql = """
    MERGE INTO aapl_timeseries AS target
    USING aapl_timeseries_temp AS source
    ON target.ticker = source.ticker
    AND target.date = source.date                 
    WHEN MATCHED THEN
        UPDATE SET
            target.[open] = source.[open],
            target.high = source.high,
            target.low = source.low,
            target.[close] = source.[close],
            target.volume = source.volume,
            target.inserted_at = source.inserted_at
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (date, [open], high, low, [close], volume, ticker, inserted_at)
        VALUES (source.date, source.[open], source.high, source.low, source.[close], source.volume, source.ticker, source.inserted_at)
    OUTPUT 
        $action AS action_type,
        inserted.id AS new_id,
        deleted.id AS old_id,
        inserted.date AS new_date,
        deleted.date AS old_date,
        inserted.[open] AS new_open,
        deleted.[open] AS old_open,
        inserted.high AS new_high,
        deleted.high AS old_high,
        inserted.low AS new_low,
        deleted.low AS old_low,
        inserted.[close] AS new_close,
        deleted.[close] AS old_close,
        inserted.volume AS new_volume,
        deleted.volume AS old_volume,
        inserted.inserted_at AS new_inserted_at,
        deleted.inserted_at AS old_inserted_at;
    """
    connection = engine.connect()
    result = connection.execute(text(merge_sql))

    columns = ['action_type', 'new_id', 'old_id', 'new_date', 'old_date', 'new_open', 'old_open',
                'new_high', 'old_high', 'new_low', 'old_low', 'new_close', 'old_close', 'new_volume',
                   'old_volume', 'new_inserted_at',  'old_inserted_at']
    

    changes = [dict(zip(columns,row)) for row in result]

    df_changes = pd.DataFrame(changes)

    return df_changes


df = merge_tables('timeseries')


def changes_log_to_csv(df):
    df.to_csv('aapl_timeseries_changes_log.csv', index = False)


changes_log_to_csv(df)


#For file names (random number generator): import uuid 