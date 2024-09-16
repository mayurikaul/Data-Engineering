import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from alpha_vantage_utils import get_alpha_vantage_raw_data
from json_utils import upload_raw_data_to_json_temp_store, get_raw_data_from_json
from aws_utils import upload_to_aws_s3, get_data_from_s3
from sql_utils import connect_to_db, export_data_to_sql
from news_utils import process_raw_news_data

def create_news_table(sql_database:str, table_name:str):
    engine = connect_to_db(sql_database)

    Base = declarative_base()

    class NewsData(Base):
        __tablename__ = table_name

        id = Column(Integer, primary_key=True, autoincrement=True)
        title = Column(String(500), nullable=False)
        url = Column(String(2083), nullable = False)
        time_published = Column(DateTime, nullable = False)
        authors = Column(String)
        summary = Column(String, nullable = False)
        source = Column(String, nullable = False)
        topics = Column(String)
        overall_sentiment_score = Column(Float, nullable = False)
        overall_sentiment_label = Column(String, nullable=False)
        ticker_sentiment = Column(String, nullable= False)
        ticker = Column(String(5),nullable=False)
        inserted_at = Column(DateTime, nullable=False)

    Base.metadata.create_all(engine)


#create_news_table('news_data', 'aapl_news')


def single_job(ticker:str):
    #url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&limit=50&apikey=AFJ71MOJKQYF8WN4"
    #data_av = get_alpha_vantage_raw_data(url)
    #upload_raw_data_to_json_temp_store(data_av, f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_news.json')
    data_j = get_raw_data_from_json(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_news.json')
    df_news = process_raw_news_data(data_j)
    upload_to_aws_s3(df_news, 'news-dataset', f'{ticker}_news.csv')
    df_news = get_data_from_s3('news-dataset', f'{ticker}_news.csv')
    export_data_to_sql('news_data', df_news, f'{ticker.lower()}_news')


#single_job('AAPL')


# To create an initial (empty)changes log to track changes:
# We do not run this every time, since it would create a new changes log, deleting the previous entries. 
# We run this once when setting up the data for a ticker. 

def create_changes_log(ticker:str):
    columns = ['action_type', 'new_id', 'old_id', 
               'new_title', 'old_title', 
               'new_url', 'old_url',
                'new_time_published', 'old_time_published', 
                'new_authors', 'old_authors', 
                'new_summary', 'old_summary', 
                'new_source','old_source', 
                'new_topics', 'old_topics',
                'new_overall_sentiment_score', 'old_overall_sentiment_score', 
                'new_overall_sentiment_label', 'old_overall_sentiment_label', 
                'new_ticker_sentiment', 'old_ticker_sentiment', 
                'new_inserted_at',  'old_inserted_at']

    changes_log = pd.DataFrame(columns=columns)
    changes_log.to_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_news_changes_log.csv', index = False)

#create_changes_log('AAPL')
