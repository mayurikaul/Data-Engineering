import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
import sys

sys.path.append('/Users/mayurikaul/Desktop/DataEngineering/Neptune/Utils_Scripts')

from sql_utils import connect_to_db

def process_raw_news_data(data):
    news = data["feed"]
    df_news = pd.DataFrame.from_dict(news)
    df_news.drop(columns=['banner_image', 'category_within_source', 'source_domain'], inplace=True)

    df_news['time_published'] = pd.to_datetime(df_news['time_published'], format='%Y%m%dT%H%M%S').dt.strftime('%Y/%m/%d')

    df_news['Ticker'] = 'AAPL'
    
    df_news['inserted_at'] = pd.to_datetime(datetime.now(timezone.utc))
    df_news['inserted_at'] = df_news['inserted_at'].apply(lambda x: x.replace(microsecond=0))
    df_news['inserted_at'] = df_news['inserted_at'].dt.tz_localize(None)
    
    return df_news


def create_temp_and_merge_news(ticker:str, sql_database:str, df):
    engine = connect_to_db(sql_database)

    with engine.begin() as connection:
        
        connection.execute(text("DROP TABLE IF EXISTS #temp_newsdata;"))

        connection.execute(text("""
            CREATE TABLE #temp_newsdata (
                id INT PRIMARY KEY IDENTITY(1,1),
                title VARCHAR(500) NOT NULL,
                url NVARCHAR(2083) NOT NULL,
                time_published DATETIME NOT NULL,
                authors VARCHAR(250),
                summary VARCHAR(1000) NOT NULL, 
                source VARCHAR(100) NOT NULL, 
                topics VARCHAR(1000),
                overall_sentiment_score FLOAT NOT NULL,
                overall_sentiment_label VARCHAR(100) NOT NULL,
                ticker_sentiment VARCHAR(2500) NOT NULL, 
                ticker VARCHAR(5) NOT NULL,
                inserted_at DATETIME NOT NULL
                );
            """))
        
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        insert_sql = """
        INSERT INTO #temp_newsdata (title, url, time_published, authors, summary, source, topics, overall_sentiment_score, overall_sentiment_label, ticker_sentiment, ticker, inserted_at)
        VALUES (:title, :url, :time_published, :authors, :summary, :source, :topics, :overall_sentiment_score, :overall_sentiment_label, :ticker_sentiment, :ticker, :inserted_at);
        """

        for index,row in df.iterrows():
            connection.execute(text(insert_sql), {
                'title' : row['title'],
                'url' : row['url'] , 
                'time_published' : row['time_published'], 
                'authors' : row['authors'], 
                'summary' : row['summary'], 
                'source' : row['source'], 
                'topics' : row['topics'], 
                'overall_sentiment_score' : row['overall_sentiment_score'], 
                'overall_sentiment_label' : row['overall_sentiment_label'], 
                'ticker_sentiment' : row['ticker_sentiment'], 
                'ticker' : row['Ticker'], 
                'inserted_at' : row['inserted_at']
            })
        
        
        merge_sql = f"""
        MERGE INTO {ticker.lower()}_news AS target
        USING #temp_newsdata AS source
        ON target.ticker = source.ticker
        AND LOWER(TRIM(REPLACE(REPLACE(REPLACE(target.title, ',', ''), '.', ''), '!', ''))) = 
            LOWER(TRIM(REPLACE(REPLACE(REPLACE(source.title, ',', ''), '.', ''), '!', '')))                 
        WHEN MATCHED AND 
            (target.url != source.url OR 
            target.time_published != source.time_published OR 
            target.authors != source.authors OR 
            target.summary != source.summary OR 
            target.[source] != source.[source] OR
            target.overall_sentiment_score != source.overall_sentiment_score OR
            target.overall_sentiment_label != source.overall_sentiment_label)
        THEN
            UPDATE SET
                target.url = source.url,
                target.time_published = source.time_published, 
                target.authors = source.authors,
                target.summary = source.summary, 
                target.[source] = source.[source],
                target.overall_sentiment_score = source.overall_sentiment_score,
                target.overall_sentiment_label = source.overall_sentiment_label,
                target.inserted_at = source.inserted_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (title, url, time_published, authors, summary, source, topics, overall_sentiment_score, overall_sentiment_label, ticker_sentiment, ticker, inserted_at)
            VALUES (source.title, source.url, source.time_published, source.authors, source.summary, source.[source], source.topics, source.overall_sentiment_score, source.overall_sentiment_label, source.ticker_sentiment, source.ticker, source.inserted_at)
        OUTPUT 
            $action AS action_type,
            inserted.id AS new_id,
            deleted.id AS old_id,
            inserted.title AS new_title,
            deleted.title AS old_title,
            inserted.url AS new_url,
            deleted.url AS old_url,
            inserted.time_published AS new_time_published,
            deleted.time_published AS old_time_published,
            inserted.authors AS new_authors,
            deleted.authors AS old_authors,
            inserted.summary AS new_summary,
            deleted.summary AS old_summary,
            inserted.source AS new_source,
            deleted.source AS old_source,
            inserted.topics AS new_topics,
            deleted.topics AS old_topics,
            inserted.overall_sentiment_score AS new_overall_sentiment_score,
            deleted.overall_sentiment_score AS old_overall_sentiment_score,
            inserted.overall_sentiment_label AS new_overall_sentiment_label,
            deleted.overall_sentiment_label AS old_overall_sentiment_label,
            inserted.ticker_sentiment AS new_ticker_sentiment,
            deleted.ticker_sentiment AS old_ticker_sentiment,
            inserted.inserted_at AS new_inserted_at,
            deleted.inserted_at AS old_inserted_at;
        """
    
        result = connection.execute(text(merge_sql))
        output = result.fetchall()
    
    return output


def log_news_changes(result, ticker:str):
    if not result:
        print("No changes detected.")
        return
    
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
    
    changes = [dict(zip(columns,row)) for row in result]
    df_changes = pd.DataFrame(changes)
    changes_log = pd.read_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_news_changes_log.csv')
    updated_changes_log = pd.concat([changes_log, df_changes], ignore_index=True)
    updated_changes_log.to_csv(f'~/Desktop/DataEngineering/Neptune/ChangesLog/{ticker}_news_changes_log.csv', index = False)



