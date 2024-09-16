import pandas as pd
import numpy as np
from datetime import datetime, timezone
from sqlalchemy import text
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from sql_utils import connect_to_db


def process_earnings(data):
    yearly = data["annualEarnings"]
    df_yearly = pd.DataFrame.from_dict(yearly) 
    df_yearly.rename(columns={'fiscalDateEnding' : 'fiscal_date_ending',
                                 'reportedEPS' : 'reported_eps'}, inplace=True)

    df_yearly["Ticker"] = "AAPL"
    df_yearly['inserted_at'] = pd.to_datetime(datetime.now(timezone.utc))
    df_yearly['inserted_at'] = df_yearly['inserted_at'].apply(lambda x: x.replace(microsecond=0))
    df_yearly['inserted_at'] = df_yearly['inserted_at'].dt.tz_localize(None)


    quarterly = data["quarterlyEarnings"]
    df_quarterly = pd.DataFrame.from_dict(quarterly)
    df_quarterly.rename(columns={'fiscalDateEnding' : 'fiscal_date_ending',
                                 'reportedDate' : 'reported_date',
                                 'reportedEPS' : 'reported_eps',
                                 'estimatedEPS' : 'estimated_eps',
                                 'surprisePercentage' : 'surprise_perc',
                                 'reportTime' : 'report_time'}, inplace=True)
    df_quarterly["Ticker"] = "AAPL"
    df_quarterly['inserted_at'] = pd.to_datetime(datetime.now(timezone.utc))
    df_quarterly['inserted_at'] = df_quarterly['inserted_at'].apply(lambda x: x.replace(microsecond=0))
    df_quarterly['inserted_at'] = df_quarterly['inserted_at'].dt.tz_localize(None)

    return df_yearly,df_quarterly


def create_temp_and_merge_yearly(ticker:str, sql_database:str, df):
    engine = connect_to_db(sql_database)

    with engine.begin() as connection:

        connection.execute(text("DROP TABLE IF EXISTS #temp_yearly_earnings;"))

        connection.execute(text("""
                CREATE TABLE #temp_yearly_earnings (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    fiscal_date_ending DATETIME NOT NULL,
                    reported_eps FLOAT NOT NULL, 
                    ticker VARCHAR(5) NOT NULL,
                    inserted_at DATETIME NOT NULL
                );
                                             
            """))
        
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        insert_sql = """
        INSERT INTO #temp_yearly_earnings (fiscal_date_ending, reported_eps, ticker, inserted_at )
        VALUES (:fiscal_date_ending, :reported_eps, :ticker, :inserted_at);
        """

        for index, row in df.iterrows():
            connection.execute(text(insert_sql), {
                'fiscal_date_ending': row['fiscal_date_ending'],
                'reported_eps': row['reported_eps'],
                'ticker': row['Ticker'],
                'inserted_at': row['inserted_at']
            })


        merge_sql = f"""
        MERGE INTO {ticker.lower()}_yearly AS target
        USING #temp_yearly_earnings AS source
        ON target.ticker = source.ticker
        AND target.fiscal_date_ending = source.fiscal_date_ending                
        WHEN MATCHED AND 
            (target.reported_eps != source.reported_eps)
        THEN
            UPDATE SET
                target.reported_eps = source.reported_eps,
                target.inserted_at = source.inserted_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (fiscal_date_ending, reported_eps, ticker, inserted_at)
            VALUES (source.fiscal_date_ending, source.reported_eps, source.ticker, source.inserted_at)
        OUTPUT 
            $action AS action_type,
            inserted.id AS new_id,
            deleted.id AS old_id,
            inserted.fiscal_date_ending AS new_fiscal_date_ending,
            deleted.fiscal_date_ending AS old_fiscal_date_ending,
            inserted.reported_eps AS new_reported_eps,
            deleted.reported_eps AS old_reported_eps,
            inserted.inserted_at AS new_inserted_at,
            deleted.inserted_at AS old_inserted_at;
        """

        result = connection.execute(text(merge_sql))
        output = result.fetchall()

    return output



#NTS: This is the same for all datasets bar the columns, seems redundant (DRY).
# Think about how to improve.         
def log_yearly_changes(result, ticker:str):
    if not result:
        print("No changes detected.")
        return
    
    columns_yearly = ['action_type', 'new_id', 'old_id', 'new_fiscal_date_ending',
                  'old_fiscal_date_ending', 'new_reported_eps', 'old_reported_eps',
                   'new_inserted_at', 'old_inserted_at']
    
    changes = [dict(zip(columns_yearly,row)) for row in result]
    df_changes = pd.DataFrame(changes)
    changes_log = pd.read_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_yearly_earnings_changes_log.csv')
    updated_changes_log = pd.concat([changes_log, df_changes], ignore_index=True)
    updated_changes_log.to_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_yearly_earnings_changes_log.csv', index = False)


def create_temp_and_merge_quarterly(ticker:str, sql_database:str, df):
    engine = connect_to_db(sql_database)

    df = df.replace({np.nan: None})

    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS #temp_quarterly_earnings;"))

        connection.execute(text("""
                CREATE TABLE #temp_quarterly_earnings (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    fiscal_date_ending DATETIME NOT NULL,
                    reported_date DATETIME NOT NULL,
                    reported_eps FLOAT NOT NULL, 
                    estimated_eps FLOAT,
                    surprise FLOAT,
                    surprise_perc FLOAT,
                    report_time VARCHAR(50),
                    ticker VARCHAR(5) NOT NULL,
                    inserted_at DATETIME NOT NULL
                );                              
            """))
        
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        insert_sql = """
        INSERT INTO #temp_quarterly_earnings (fiscal_date_ending, reported_date, reported_eps, estimated_eps, surprise, surprise_perc, report_time, ticker, inserted_at )
        VALUES (:fiscal_date_ending, :reported_date, :reported_eps, :estimated_eps, :surprise, :surprise_perc, :report_time, :ticker, :inserted_at );
        """

        for index, row in df.iterrows():
            connection.execute(text(insert_sql), {
                'fiscal_date_ending': row['fiscal_date_ending'],
                'reported_date' : row['reported_date'],
                'reported_eps': row['reported_eps'],
                'estimated_eps': row['estimated_eps'],
                'surprise': row['surprise'],
                'surprise_perc': row['surprise_perc'],
                'report_time' : row['report_time'],
                'ticker': row['Ticker'],
                'inserted_at': row['inserted_at']
            })


        merge_sql = f"""
        MERGE INTO {ticker.lower()}_quarterly AS target
        USING #temp_quarterly_earnings AS source
        ON target.ticker = source.ticker
        AND target.fiscal_date_ending = source.fiscal_date_ending                
        WHEN MATCHED AND 
            (target.reported_date != source.reported_date OR
            target.reported_eps != source.reported_eps OR
            target.estimated_eps != source.estimated_eps OR
            target.surprise != source.surprise OR
            target.surprise_perc != source.surprise_perc OR
            target.report_time != source.report_time)
        THEN
            UPDATE SET
                target.reported_date = source.reported_date,
                target.reported_eps = source.reported_eps,
                target.estimated_eps = source.estimated_eps,
                target.surprise = source.surprise,
                target.surprise_perc = source.surprise_perc,
                target.report_time = source.report_time,
                target.inserted_at = source.inserted_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (fiscal_date_ending, reported_date, reported_eps, estimated_eps, surprise, surprise_perc, report_time, ticker, inserted_at)
            VALUES (source.fiscal_date_ending, source.reported_date, source.reported_eps, source.estimated_eps, source.surprise, source.surprise_perc, source.report_time, source.ticker, source.inserted_at)
        OUTPUT 
            $action AS action_type,
            inserted.id AS new_id,
            deleted.id AS old_id,
            inserted.fiscal_date_ending AS new_fiscal_date_ending,
            deleted.fiscal_date_ending AS old_fiscal_date_ending,
            inserted.reported_date AS new_reported_date,
            deleted.reported_date AS old_reported_date,
            inserted.reported_eps AS new_reported_eps,
            deleted.reported_eps AS old_reported_eps,
            inserted.estimated_eps AS new_estimated_eps,
            deleted.estimated_eps AS old_estimated_eps,
            inserted.surprise AS new_surprise,
            deleted.surprise AS old_surprise,
            inserted.surprise_perc AS new_surprise_perc,
            deleted.surprise_perc AS old_surprise_perc,
            inserted.report_time AS new_report_time,
            deleted.report_time AS old_report_time,
            inserted.inserted_at AS new_inserted_at,
            deleted.inserted_at AS old_inserted_at;
        """
    
        result = connection.execute(text(merge_sql))
        output = result.fetchall()

    return output


def log_quarterly_changes(result, ticker:str):
    if not result:
        print("No changes detected.")
        return
    
    columns_quarterly = ['action_type', 'new_id', 'old_id', 'new_fiscal_date_ending', 'old_fiscal_date_ending', 'new_reported_date', 
                        'old_reported_date', 'new_reported_eps', 'old_reported_eps', 'new_estimated_eps', 'old_estimated_eps',
                        'new_surprise', 'old_surprise', 'new_surprise_perc', 'old_surprise_perc', 'new_report_time', 'old_report_time',
                        'new_inserted_at', 'old_inserted_at']
    
    changes = [dict(zip(columns_quarterly,row)) for row in result]
    df_changes = pd.DataFrame(changes)
    changes_log = pd.read_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_quarterly_earnings_changes_log.csv')
    updated_changes_log = pd.concat([changes_log, df_changes], ignore_index=True)
    updated_changes_log.to_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_quarterly_earnings_changes_log.csv', index = False)








