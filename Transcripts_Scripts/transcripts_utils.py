import requests
import json
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from json_utils import upload_raw_data_to_json_temp_store, get_raw_data_from_json
from sql_utils import connect_to_db

#Using Rapid API for Seeking Alpha: api_key = 'f13c7bbe1emshb2acabefe1c2ec0p175bb9jsn0732e8dd59f0'


#The following gives a list of transcripts for AAPL: "https://seeking-alpha.p.rapidapi.com/transcripts/v2/list"
def get_raw_transcript_list(ticker:str, size:int, number:int):
     api_key = 'f13c7bbe1emshb2acabefe1c2ec0p175bb9jsn0732e8dd59f0'
     url = "https://seeking-alpha.p.rapidapi.com/transcripts/v2/list"
     querystring = {"id": ticker,"size": size,"number": number}
     headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
     }
     try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()
        return data
     except requests.exceptions.RequestException as e:
        print(f"An error ocurred: {e}")
     except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")



#Gets a list of transcript ids for each ticker
def get_transcript_list(ticker:str):
    #av_transcript_data = get_raw_transcript_list(f'{ticker}', 40, 1)
    #upload_raw_data_to_json_temp_store(av_transcript_data, f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_transcript_list.json')
    transcript_data = get_raw_data_from_json(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/json_files/{ticker}_transcript_list.json')
    transcript_id_list = []
    for i in range(len(transcript_data['data'])):
       transcript_id_list.append(transcript_data['data'][i]['id'])

    return transcript_id_list


#Getting the actual of the transcripts: "https://seeking-alpha.p.rapidapi.com/transcripts/v2/get-details"
def get_raw_transcript_details(transcript_id:str):
   api_key = 'f13c7bbe1emshb2acabefe1c2ec0p175bb9jsn0732e8dd59f0'
   url = "https://seeking-alpha.p.rapidapi.com/transcripts/v2/get-details"
   querystring = {"id": transcript_id}
   headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
     }
   
   response = requests.get(url, headers=headers, params=querystring)
   data = response.json()
   
   return data

def process_transcript_details(data):
   publish_date = data['data']['attributes']['publishOn']
   title = data['data']['attributes']['title']
   transcript = data['data']['attributes']['content']

   selected_data = {
     'publish_date' : [publish_date],
     'title' : [title],
     'transcript' : [transcript]
     }
   
   df_transcript = pd.DataFrame(selected_data)
   df_transcript['publish_date'] = pd.to_datetime(df_transcript['publish_date']).dt.strftime('%Y/%m/%d')
   df_transcript['ticker'] = 'AAPL'
    
   df_transcript['inserted_at'] = pd.to_datetime(datetime.now(timezone.utc))
   df_transcript['inserted_at'] = df_transcript['inserted_at'].apply(lambda x: x.replace(microsecond=0))
   df_transcript['inserted_at'] = df_transcript['inserted_at'].dt.tz_localize(None)
    
   return df_transcript



def create_temp_transcript_df(ticker:str):
   columns = ['publish_date', 'title', 'transcript', 'ticker', 'inserted_at']
   all_transcripts_df = pd.DataFrame(columns=columns)
   all_transcripts_df.to_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/Transcripts/{ticker}_transcripts_temp.csv', index = False)


create_temp_transcript_df("AAPL")



def create_temp_and_merge_transcripts(ticker:str, sql_database:str, df):
   engine = connect_to_db(sql_database)

   with engine.begin() as connection:
      connection.execute(text("DROP TABLE IF EXISTS #temp_transcripts;"))

      connection.execute(text("""
            CREATE TABLE #temp_transcripts (
                  id INT PRIMARY KEY IDENTITY(1,1),
                  publish_date DATETIME NOT NULL, 
                  title VARCHAR(500) NOT NULL,
                  transcript VARCHAR(MAX) NOT NULL, 
                  ticker VARCHAR(5) NOT NULL, 
                  inserted_at DATETIME NOT NULL
                  );
            """))
      
      df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

      insert_sql = """
      INSERT INTO #temp_transcripts (publish_date, title, transcript, ticker, inserted_at)
      VALUES (:publish_date, :title, :transcript, :ticker, :inserted_at);
      """

      for index,row in df.iterrows():
         connection.execute(text(insert_sql), {
            'publish_date' : row['publish_date'],
            'title' : row['title'],
            'transcript' : row['transcript'],
            'ticker' : row['ticker'],
            'inserted_at' : row['inserted_at']
         })


      merge_sql = f"""
      MERGE INTO {ticker.lower()}_transcripts AS TARGET
      USING #temp_transcripts AS SOURCE
      ON target.ticker = source.ticker
      AND target.publish_date = source.publish_date
      WHEN MATCHED AND
         (target.title != source.title OR
         target.transcript != source.transcript)
      THEN 
         UPDATE SET
            target.title = source.title, 
            target.transcript = source.transcript,
            target.inserted_at = source.inserted_at
      WHEN NOT MATCHED BY TARGET THEN
         INSERT (publish_date, title, transcript, ticker, inserted_at)
         VALUES (source.publish_date, source.title, source.transcript, source.ticker, source.inserted_at)
      OUTPUT
         $action AS action_type,
         inserted.id AS new_id,
         deleted.id AS old_id,
         inserted.publish_date AS new_publish_date,
         deleted.publish_date AS old_publish_date,
         inserted.title AS new_title,
         deleted.title AS old_title,
         inserted.transcript AS new_transcript,
         deleted.transcript AS old_transcript,
         inserted.inserted_at AS new_inserted_at,
         deleted.inserted_at AS old_inserted_at;
      """

      result = connection.execute(text(merge_sql))
      output = result.fetchall()
   
   return output


def log_transcripts_changes(result, ticker:str):
   if not result:
      print("No changes detected.")
      return
   
   columns = ['action_type', 'publish_date', 'title', 'transcript', 'ticker', 'inserted_at']
   changes = [dict(zip(columns, row)) for row in result]
   df_changes = pd.DataFrame(changes)
   changes_log = pd.read_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_transcripts_changes_log.csv')
   updated_changes_log = pd.concat([changes_log, df_changes], ignore_index=True)
   updated_changes_log.to_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_transcripts_changes_log.csv', index=False)




        






