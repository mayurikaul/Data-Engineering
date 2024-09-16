import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from aws_utils import upload_to_aws_s3, get_data_from_s3
from sql_utils import connect_to_db, export_data_to_sql
from transcripts_utils import get_transcript_list, get_raw_transcript_details, process_transcript_details


#Creating an empty transcript df, one time for each ticker
def create_transcript_df(ticker:str):
   columns = ['publish_date', 'title', 'transcript', 'ticker', 'inserted_at']
   all_transcripts_df = pd.DataFrame(columns=columns)
   all_transcripts_df.to_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/Transcripts/{ticker}_transcripts.csv', index = False)

#create_transcript_df('AAPL')  


#Creating an empty transcript df for the temp data
def create_temp_transcript_df(ticker:str):
   columns = ['publish_date', 'title', 'transcript', 'ticker', 'inserted_at']
   all_transcripts_df = pd.DataFrame(columns=columns)
   all_transcripts_df.to_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/Transcripts/{ticker}_transcripts_temp.csv', index = False)


#create_temp_transcript_df('AAPL')



#Creating the sql transcript table for each ticker
def create_transcript_table(sql_database:str, table_name:str):
   engine = connect_to_db(sql_database)

   Base = declarative_base()

   class TranscriptData(Base):
      __tablename__ = table_name

      id = Column(Integer, primary_key=True, autoincrement=True)
      publish_date = Column(DateTime, nullable=False)
      title = Column(String(500), nullable=False)
      transcript = Column(String, nullable=False)
      ticker = Column(String(5),nullable=False)
      inserted_at = Column(DateTime, nullable=False)

   Base.metadata.create_all(engine)


#create_transcript_table('transcripts', 'aapl_transcripts')


def create_changes_log(ticker:str):
    columns = ['action_type', 'publish_date', 'title', 'transcript', 'ticker', 'inserted_at']
    changes_log = pd.DataFrame(columns=columns)
    changes_log.to_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_transcripts_changes_log.csv', index = False)


#create_changes_log('AAPL')

 
#Getting the transcript details for each ticker
def single_job(ticker:str):
   list_of_transcripts = get_transcript_list(ticker)
   all_transcripts_df = pd.read_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/Transcripts/{ticker}_transcripts.csv')

   for i in range(len(list_of_transcripts)):
      transcript = get_raw_transcript_details(list_of_transcripts[i])
      df_transcript = process_transcript_details(transcript)
      all_transcripts_df = pd.concat([all_transcripts_df, df_transcript], ignore_index=True)

   upload_to_aws_s3(all_transcripts_df, 'transcript-reports', f'{ticker}_transcripts.csv')
   df_transcript = get_data_from_s3('transcript-reports', f'{ticker}_transcripts.csv')
   export_data_to_sql('transcripts', df_transcript, f'{ticker.lower()}_transcripts')


#single_job('AAPL')