import requests
import json
import pandas as pd
from io import StringIO
import boto3
import functions

#Using Rapid API for Seeking Alpha
api_key = 'f13c7bbe1emshb2acabefe1c2ec0p175bb9jsn0732e8dd59f0'

#The following gives a list of transcripts for AAPL
url1 = "https://seeking-alpha.p.rapidapi.com/transcripts/v2/list"


def get_raw_transcript_list(url, ticker, size, number):
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


def get_transcript_list():
    av_transcript_data = get_raw_transcript_list(url1, "aapl", 40, 1)
    functions.upload_raw_data_to_json_temp_store(av_transcript_data, "AAPL_transcript_list.json")
    transcript_data = functions.get_raw_data_from_json("AAPL_transcript_list.json")
    transcript_id_list = []
    for i in range(len(transcript_data['data'])):
       transcript_id_list.append(transcript_data['data'][i]['id'])

    return transcript_id_list


 
#Now getting the details of the transcripts
url2 = "https://seeking-alpha.p.rapidapi.com/transcripts/v2/get-details"


def get_raw_transcript_details(url, id:str):
   querystring = {"id":id}
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
     'Publish Date' : [publish_date],
     'Title' : [title],
     'Transcript' : [transcript]
     }
   
   df_transcript = pd.DataFrame(selected_data)

   return df_transcript


def get_transcript():
   raw_transcript_av = get_raw_transcript_details(url2, "4709458")
   functions.upload_raw_data_to_json_temp_store(raw_transcript_av, "AAPL_2024_Q3.json")
   raw_transcript_json = functions.get_raw_data_from_json("AAPL_2024_Q3.json")
   df_transcript = process_transcript_details(raw_transcript_json)
   functions.upload_to_aws_s3(df_transcript, 'transcript-reports', 'AAPL-2024-Q3.csv')
   df_transcript = functions.get_data_from_s3('transcript-reports', 'AAPL-2024-Q3.csv')
   functions.import_data_to_sql('transcripts', df_transcript, 'aapl_transcripts')

   
get_transcript()

#To-do:

# Get transcripts going back to March 2022 - at a later date though 


   
