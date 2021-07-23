import sqlite3
from binance.client import Client
import pandas as pd
import datetime
import time
import sys
from src.download_ticker_data import gather_coin_data, download_ticker_data, get_tickers
from src.db_tools import create_table, insert_dataframe, get_latest_date


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

    while True:
        
        try:
            max_date = pd.to_datetime(get_latest_date(conn, "coin_data")[0][0])
        except sqlite3.OperationalError:
            max_date = datetime.datetime.today()  

        prev_day = max_date + datetime.timedelta(days = -1)
        next_hour = max_date + datetime.timedelta(hours = 1) #+ datetime.timedelta(minutes = 1) # only download data after one minute past hour
        

        now = datetime.datetime.today()#.strftime("%H:%M:%S")

        if datetime.datetime.today() > next_hour:

            print(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

            print("\nDownloading next batch\n")
            coin_data = download_ticker_data(binance_client, coin_names, str(prev_day), frequency = "hourly")
            coin_data = coin_data.set_index(["date", "ticker"]).reset_index()

            coin_data["date"] = list(map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"), coin_data["date"]))

            print(coin_data.shape)
            insert_dataframe(conn, "coin_data", coin_data, create_if_dont_exist=True)



        else:
            sys.stdout.write('.')
            time.sleep(60)