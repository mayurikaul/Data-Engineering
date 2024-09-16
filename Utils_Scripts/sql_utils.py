from sqlalchemy import create_engine

def connect_to_db(sql_database:str):
    username = 'sa'
    password = 'OldRectory1'
    server = 'localhost'
    driver = 'ODBC Driver 17 for SQL Server'
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{sql_database}?driver={driver}"
    engine = create_engine(connection_string)

    return engine


def export_data_to_sql(sql_database:str, df, sql_table:str):
    engine = connect_to_db(sql_database)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.to_sql(sql_table, con=engine, if_exists='append', index=False)


