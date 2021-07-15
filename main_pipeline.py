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

from src.download_ticker_data import download_and_save_ticker_data


if __name__ == "__main__":
	with open("./config/binance_secret_key.txt") as f:
	    api_secret = f.readline()

	with open("./config/binance_api_key.txt") as f:
	    api_key = f.readline()

	client = Client(api_key, api_secret)


	with open("./config/tickers.txt") as f:
	    bitcoin_tickers = f.read().splitlines() 

	with open("./config/tickert_to_ignore.txt") as f:
		ignore_tickers = f.read().splitlines() 

	bitc_data = download_and_save_ticker_data(client, bitcoin_tickers, from_date= "1 Jan, 2021", frequency="hourly")
	bitc_data = bitc_data.set_index(["date", "ticker"])


	bitc_data.to_csv("./data/coin_hourly_ticker_data.csv")
