

def insert_data():
    from airflow.hooks.base import BaseHook
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine
    

    connection = BaseHook.get_connection("postgres_conn").get_uri()
    # connection = "postgresql://store:store@localhost:5433/store"
    connection = connection.replace("postgres" , "postgresql" , 1)
    db = create_engine(connection)
    conn = db.connect()

    df = pd.read_csv(
        "/usr/local/airflow/clean-files/clean_store_transactions.csv")
    df.to_sql('clean_store_transactions', con=conn, if_exists='replace',
              index=False)
    conn.close()
