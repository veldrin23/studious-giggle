import sqlite3
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
import datetime
import time
import sys
from tqdm import tqdm
import pytz
from multiprocessing import Pool, cpu_count
from itertools import product
import os
from requests.exceptions import ConnectionError
from src.download_ticker_data import get_tickers, gather_coin_data
from src.db_tools import create_table, insert_dataframe, get_latest_date
from src.check_latest_binance_date import check_latest_binance_date

def download_and_save_coin_data(binance_client, coin_name, from_date="2021-01-01 00:00:00"):

    sql_connection = sqlite3.connect("./data/crypto.db")

    coin_data = gather_coin_data(binance_client, coin_name, str(from_date), frequency = "hourly")
    # coin_data = coin_data.set_index(["date", "ticker"]).reset_index()

    coin_data["date"] = list(map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"), coin_data["date"]))
    insert_dataframe(sql_connection, "coin_data", coin_data, create_if_dont_exist=True)



def main():
    time.sleep(1) # sleep 1 second, since cron can only start on the hour
    download_succeeded = False
    tz = pytz.timezone("Australia/Sydney")
    
    cores = cpu_count()


    # get api keys
    with open("./config/binance_secret_key.txt") as f:
        api_secret = f.readline().strip("\n")
    with open("./config/binance_api_key.txt") as f:
        api_key = f.readline().strip("\n")

    # set up binance client
    binance_client = Client(api_key, api_secret)

    if os.path.exists("./data/crypto.db"):
        conn = sqlite3.connect("./data/crypto.db")
        cur = conn.cursor()
        max_date_downloaded = cur.execute("select max(date) from coin_data").fetchall()[0][0]

        # check if it's been more than an hour since the latest date in the SQL db
        # using today(), since I'm doing a naive date comparison
        if datetime.datetime.today() > pd.to_datetime(max_date_downloaded) + datetime.timedelta(hours=1):
            # if it is, check if binance has the latest data available. If not, wait 5 seconds 


            try: 
            # get list of coin names 
            coin_names = get_tickers()

            # get the latest date of the data available on binance
            latest_date_availabe = check_latest_binance_date(binance_client)

            coin_data_updated = False

            while not coin_data_updated:
                try: 
                # check if data is available 
                    if datetime.datetime.now(tz) > latest_date_availabe: 
                        print("Next batch available")
                        # download past day's worth of data 
                        prev_day = (pd.to_datetime(max_date_downloaded) + datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")

                        # split the task over several cores (TODO: see if you can use async instead)
                        with Pool(cores) as p:
                            p.starmap(download_and_save_coin_data, product([binance_client], coin_names, [prev_day]))

                        # exit condition for while loop
                        coin_data_updated = True
                                    # wait 1 second before checking again 
                    else:
                        time.sleep(1)
                        sys.stdout.write(".")
                        latest_date_availabe = check_latest_binance_date(binance_client)
                except ConnectionError:
                    coin_data_updated = False


        else:
            print("Next batch of data not available yet")
    else:
        print("No existing data found, downloading from 2021-01-01 00:00:00")
        coin_names = get_tickers()
        for coin_name in coin_names:
            print(coin_name)
            try:
                download_and_save_coin_data(binance_client, coin_name)
            except BinanceAPIException:
                print(f"{coin_name} not found")
        # with Pool(cores) as p:
        #     tqdm(p.starmap(download_and_save_coin_data, product([binance_client], coin_names, ["2021-05-25 16:00:00"])))


        


if __name__ == "__main__":
    main()


