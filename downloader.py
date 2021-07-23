import sqlite3
from binance.client import Client
import pandas as pd
import datetime
import time
import sys
from src.download_ticker_data import gather_coin_data, download_ticker_data, get_tickers
from src.db_tools import create_table, insert_dataframe, get_latest_date
import pytz

if __name__ == "__main__":
    """

    skryf documentation, ANDRE!!!

    """




    conn = sqlite3.connect("./data/crypto.db")

    with open("./config/binance_secret_key.txt") as f:
        api_secret = f.readline().strip("\n")
    with open("./config/binance_api_key.txt") as f:
        api_key = f.readline().strip("\n")

    coin_names = get_tickers()

    binance_client = Client(api_key, api_secret)

    tz = pytz.timezone("Australia/Sydney")
    while True:

        try:
            max_date = pd.to_datetime(get_latest_date(conn, "coin_data")[0][0])
            prev_day = max_date + datetime.timedelta(days = -1)
            next_hour = max_date + datetime.timedelta(hours = 1)             
        except sqlite3.OperationalError:
            print("./data/crypto.db does not exist yet, downloading data for past 31 days")
            max_date = datetime.datetime.today(tz)  
            prev_day = max_date + datetime.timedelta(days = -31)
            next_hour = max_date + datetime.timedelta(minutes = -1) 
        



        if datetime.datetime.today(tz) > next_hour:


            print("\nDownloading next batch\n", datetime.datetime.today(tz).strftime("%Y-%m-%d %H:%M:%S"))
            coin_data = download_ticker_data(binance_client, coin_names, str(prev_day), frequency = "hourly")
            coin_data = coin_data.set_index(["date", "ticker"]).reset_index()

            coin_data["date"] = list(map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"), coin_data["date"]))

            print(coin_data.shape)
            insert_dataframe(conn, "coin_data", coin_data, create_if_dont_exist=True)



        else:
            sys.stdout.write('.')
            time.sleep(60)