import os
from typing import Tuple
import pymysql
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

load_dotenv()
HOST = os.getenv("HOST")
DB_NAME = os.getenv("DB_NAME")
USERNAME = "admin"
PASSWORD = os.getenv("ADMIN_PASSWORD")


def fill_queue(symbol: str) -> None:
    """
    backfill database from current date to beginning of database entries
    ie (...)
    """
    connection = pymysql.connect(
        host = HOST,
        user = USERNAME,
        password = PASSWORD,
        db = DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    table = "queue"
    start_date = datetime.now()
    end_date = datetime(2000, 12, 1) # default end date
    current_date = start_date
    time_increment = relativedelta(months=-1)

    try: 
        with connection.cursor() as cursor:
            while current_date >= end_date:
                year = current_date.year
                month = current_date.month
                date = f"{year:04d}-{month:02d}"

                add_item_sql = f"""
                INSERT INTO {table} (symbol, date)
                VALUES (\'{symbol}\', \'{date}\');"""

                cursor.execute(add_item_sql)

                current_date += time_increment

        connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    # make table of unique symbols: V, MA, KO, PEP
    stocks = pd.read_excel('indices/sap500.xlsx')  
    # stocks["Ticker"] = str(stocks["Ticker"])
    print(stocks["Ticker"].head(30))
    for ticker in stocks["Ticker"]:
        fill_queue(ticker)
        # print(ticker)

