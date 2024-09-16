import pandas as pd
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

#Scripts to import
from transcripts_utils import get_transcript_list, get_raw_transcript_details, process_transcript_details, create_temp_and_merge_transcripts, log_transcripts_changes
from aws_utils import upload_to_aws_s3, get_data_from_s3


def update_job(ticker:str):
   list_of_transcripts = get_transcript_list(ticker)
   all_transcripts_df = pd.read_csv(f'/Users/mayurikaul/Desktop/DataEngineering/Neptune/Transcripts/{ticker}_transcripts_temp.csv')

   for i in range(len(list_of_transcripts)):
      transcript = get_raw_transcript_details(list_of_transcripts[i])
      df_transcript = process_transcript_details(transcript)
      all_transcripts_df = pd.concat([all_transcripts_df, df_transcript], ignore_index=True)

   upload_to_aws_s3(all_transcripts_df, 'transcript-reports', f'{ticker}_transcripts_temp.csv')
   df_transcript = get_data_from_s3('transcript-reports', f'{ticker}_transcripts_temp.csv')
   output_log = create_temp_and_merge_transcripts(ticker, 'transcripts', df_transcript)
   log_transcripts_changes(output_log, ticker)
   


update_job('AAPL')