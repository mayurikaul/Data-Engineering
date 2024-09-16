import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from sql_utils import connect_to_db

def process_raw_time_series_data(data):
    time_series = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(time_series, orient = 'index')
    df = df.reset_index()
    df.rename(columns={'index':'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    col_name_dict = {'1. open' : 'open',
                     '2. high' : 'high',
                     '3. low' : 'low',
                     '4. close' : 'close',
                     '5. volume' : 'volume'}
    df.rename(columns=col_name_dict, inplace=True) 
    df['Ticker'] = "AAPL"
    df['inserted_at'] = pd.to_datetime(datetime.now(timezone.utc))
    df['inserted_at'] = df['inserted_at'].apply(lambda x: x.replace(microsecond=0))
    df['inserted_at'] = df['inserted_at'].dt.tz_localize(None)
    return df


# def export_time_series_to_sql(sql_database:str, df, sql_table:str):
#     engine = connect_to_db(sql_database)
#     df = df.loc[:, ~df.columns.str.contains('^Unnamed')] #This is currently the reason why these functions don't generalise
#     df.to_sql(sql_table, con=engine, index=False, if_exists='append')


def create_temp_and_merge_timeseries(ticker:str, sql_database:str, df):
    engine = connect_to_db(sql_database)

    with engine.begin() as connection:

        connection.execute(text("DROP TABLE IF EXISTS #temp_timeseries;"))

        connection.execute(text("""
            CREATE TABLE #temp_timeseries (
                id INT PRIMARY KEY IDENTITY(1,1),
                date DATETIME NOT NULL,
                [open] FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                [close] FLOAT NOT NULL,
                volume INT NOT NULL,
                ticker VARCHAR(5) NOT NULL,
                inserted_at DATETIME NOT NULL
            );
        """))

        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        insert_sql = """
        INSERT INTO #temp_timeseries (date, [open], high, low, [close], volume, ticker, inserted_at)
        VALUES (:date, :open, :high, :low, :close, :volume, :ticker, :inserted_at);
        """

        for index, row in df.iterrows():
            connection.execute(text(insert_sql), {
                'date': row['Date'],
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'ticker': row['Ticker'],
                'inserted_at': row['inserted_at']
            })


        merge_sql = f"""
        MERGE INTO {ticker.lower()}_timeseries AS target
        USING #temp_timeseries AS source
        ON target.ticker = source.ticker
        AND target.date = source.date                 
        WHEN MATCHED AND 
            (target.[open] != source.[open] OR 
            target.high != source.high OR 
            target.low != source.low OR 
            target.[close] != source.[close] OR 
            target.volume != source.volume)
        THEN
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
    
        result = connection.execute(text(merge_sql))
        output = result.fetchall()
    
    return output


def log_timeseries_changes(result, ticker:str):
    if not result:
        print("No changes detected.")
        return
    
    columns = ['action_type', 'new_id', 'old_id', 'new_date', 'old_date', 'new_open', 'old_open',
                'new_high', 'old_high', 'new_low', 'old_low', 'new_close', 'old_close', 'new_volume',
                   'old_volume', 'new_inserted_at',  'old_inserted_at']
    
    changes = [dict(zip(columns,row)) for row in result]
    df_changes = pd.DataFrame(changes)
    changes_log = pd.read_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_timeseries_changes_log.csv')
    updated_changes_log = pd.concat([changes_log, df_changes], ignore_index=True)
    updated_changes_log.to_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_timeseries_changes_log.csv', index = False)

