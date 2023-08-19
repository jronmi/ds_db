"""
Docstring
"""
import os
import logging
from typing import Tuple
from datetime import timedelta
import alpyvantage as av
from dotenv import load_dotenv
import pendulum
from airflow.decorators import dag, task
import pymysql
import sqlalchemy
import pandas as pd
from time import sleep

load_dotenv()
AV_API_KEY = os.getenv("AV_API_KEY")
HOST = os.getenv("HOST")
DB_NAME = os.getenv("DB_NAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

logger = logging.getLogger("airflow.task")


@dag(
    schedule=timedelta(minutes=1),
    start_date=pendulum.datetime(2023, 8, 6, tz="EST"),
    catchup=False,
    tags=["quant"],
    max_active_runs=1,
    default_args={
        "retries": 1,
        # 'on_failure_callback':lambda _:_,
    },
)
def etl_pipeline():
    """
    ### Quant ETL Pipeline
    this is a data pipeline which extracts data from the Alpha Vantage
    API and stores it in an AWS TODO database. the data is stored in its raw form,
    so there is no 'transform' task currently
    """

    def clear(symbol: str):
        """
        #### Clear
        A function which removes all remaining entries of a given symbol from the queue.
        To be used when a syntax error or an implicit error (returns default first month)

        params:
            symbol: the symbol to be cleared
        """
        connection = pymysql.connect(
            host=HOST,
            user=USERNAME,
            password=PASSWORD,
            db=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
        )
        table = "queue"

        with connection.cursor() as cursor:
            delete_items_sql = f"""
            DELETE FROM {table}
            WHERE symbol='{symbol}'"""

            cursor.execute(delete_items_sql)

            connection.commit()
            connection.close()

    def pop_queue():
        """
        #### Pop Queue
        Determine which symbol and month pair to extract next by popping it
        from the queue

        returns: (symbol, month)
        """
        connection = pymysql.connect(
            host=HOST,
            user=USERNAME,
            password=PASSWORD,
            db=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
        )
        table = "queue"
        limit = 2
        # no try block --> so task retries
        with connection.cursor() as cursor:
            get_item_sql = f"""
            select * from {table}
            order by symbol, date desc
            limit {limit};"""

            cursor.execute(get_item_sql)

            items = cursor.fetchall()  # or fetchall

            ids = [item["id"] for item in items]
            items = [(item["symbol"], item["date"]) for item in items]

            drop_item_sql = f"""
            DELETE FROM {table}
            where id in {*ids,};"""
            print(drop_item_sql)

            cursor.execute(drop_item_sql)

            connection.commit()
            connection.close()

            return items

    def extract(items) -> pd.DataFrame:
        """
        #### Extract task
        pull data from the Alpha Vantage intraday API with the following params:
         - interval: 1min
         - adjusted: true (default)
         - extended_hours: true (default)
         - output_size: full (ie. full month of data)
         - datatype: csv

         params:
         - symbol: the stock symbol to be fetched
         - month: the month of data to be fetched. YYYY-MM format
        """
        dataframes = []
        api = av.API(AV_API_KEY)

        def call_api(symbol, month):
            data, meta_data = api.time_series_intraday(
                symbol, month=month, interval="1min", outputsize="full"
            )
            data["symbol"] = symbol
            dataframes.append(data)

        for symbol, month in items:
            try:
                call_api(symbol, month)
            except av.AlphaVantageError as e:
                print(e)
                if "frequency" in str(e):
                    sleep(60)  # sleep for 1 min if over api limit\
                    call_api(symbol, month)
                # else: interpret as syntax error

        if len(dataframes) > 0:
            return pd.concat(dataframes)
        else:
            return pd.DataFrame()  # TODO: does this make sense?

    def transform(data: pd.DataFrame):
        """
        We remove duplicate items as those in the table so that we can use
        a composit primary key (Symbol, Date)
        params:
            the data which needs to be removed of duplicates
        """


        connection = pymysql.connect(
            host=HOST,
            user=USERNAME,
            password=PASSWORD,
            db=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
        )
        table = "av_minute_data"

        with connection.cursor() as cursor:
            # duplicate date index as column for easier manipulation
            data["date"] = data.index
            keys = [(row["symbol"], row["date"]) for _, row in data.iterrows()]

            check_duplicate_sql = f"""
            SELECT symbol, date FROM {table}
            WHERE (symbol, date) in {*keys,}"""

            cursor.execute(check_duplicate_sql)
            duplicates = cursor.fetchall()
            duplicates = {(item["symbol"], item["date"]) for item in duplicates}

            data = data[~data[["symbol", "date"]].apply(tuple, axis=1).isin(duplicates)]
            # delete column to maintain shape 
            del data["date"]

            connection.commit()
            connection.close()

            return data

    def load(data: pd.DataFrame):
        """
        load dataframe into the sql table
        """
        url = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}/{DB_NAME}"
        engine = sqlalchemy.create_engine(url)
        table = "av_minute_data"
        data.to_sql(table, engine, if_exists="append")

    @task
    def fetch():
        # maybe make these tasks and store data in buffer table
        items = pop_queue()
        data = extract(items)
        data = transform(data)
        load(data)

    fetch()


etl_pipeline()
