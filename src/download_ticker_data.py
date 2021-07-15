import requests
from binance.client import Client
import pandas as pd
import time
import time
import dateparser
import pytz
import json
import datetime
from tqdm import tqdm
from glob import glob
import numpy as np
import matplotlib.pyplot as plt


# import project_helper
# import helper
# import asyncio
# import httpx


# with open("../config/binance_secret_key.txt") as f:
#     api_secret = f.readline()

# with open("../config/binance_api_key.txt") as f:
#     api_key = f.readline()

# client = Client(api_key, api_secret)


# with open("../config/tickers.txt") as f:
#     bitcoin_tickers = f.read().splitlines() 

# with open("../config/tickert_to_ignore.txt") as f:
# 	ignore_tickers = f.read().splitlines() 


def convert_unix_time_to_date(unix_time):
    s = unix_time / 1000.0
    return datetime.datetime.fromtimestamp(s).strftime('%Y-%m-%d %H:%M:%S.%f')


def create_filename_date(datetime_object):
    
    assert type(datetime_object) == datetime.datetime, "requires datetime object"
    
    year = str(datetime_object.year)
    month = str(datetime_object.month+100)[-2:]
    day = str(datetime_object.day+100)[-2:]
    
    return "_".join([year, month, day])
    

def gather_coin_date(client, ticker, from_date, frequency = "daily"):
    colnames = ["unix",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume_1",
                "Volume_2",
                "trade_count",
                "unknown_1",
                "unknown_2",
                "unknown_3",
                "unknown_4"]
    
    freq_dict = {"daily": Client.KLINE_INTERVAL_1DAY,
            "hourly": Client.KLINE_INTERVAL_1HOUR}
    
    klines = client.get_historical_klines(ticker, freq_dict[frequency], from_date)
    klines_df = pd.DataFrame(klines, columns = colnames)
    
    klines_df["date"] = pd.to_datetime(klines_df.apply(lambda x: convert_unix_time_to_date(x["unix"]), axis=1))
    klines_df = klines_df.drop(["unix"],axis=1)
    klines_df["ticker"] = ticker.replace("USDT","")
    
    return klines_df


def download_and_save_ticker_data(client, bitcoin_tickers, from_date, target_folder = "./price_data", frequency = "daily"):
    
    out_df = None
    for bitcoin_ticker in tqdm(bitcoin_tickers):
        df = gather_coin_date(client, bitcoin_ticker, from_date, frequency)
        if out_df is None:
            out_df = df
        else:
            out_df = pd.concat([out_df, df])

    
    return out_df



# bitc_data = download_and_save_ticker_data(bitcoin_tickers, from_date= "1 Jan, 2021", frequency="hourly")
# bitc_data = bitc_data.set_index(["date", "ticker"])


# bitc_data.to_csv("../data/coin_hourly_ticker_data.csv")