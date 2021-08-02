from binance.client import Client
import pandas as pd
import datetime
from src.download_ticker_data import gather_coin_data
import pytz



def check_latest_binance_date(binance_client, coin_to_test = "DOGEUSDT"):
    """
    Function to see what is the latest time of data available on binance

    Args:
        binance_client: client connector to binance
        coin_to_test: a coin to use for testing, uses doge by default

    Returns:
        Maximum date of the data available on binance
    """

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

    tz = pytz.timezone("Australia/Sydney")

    an_hour_ago = datetime.datetime.now(tz) + datetime.timedelta(hours=-1)

    an_hour_ago_utc = an_hour_ago.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

    coin_data = gather_coin_data(binance_client, coin_to_test, an_hour_ago_utc, frequency = "hourly")

    max_hour = max(coin_data["date"]).tz_localize("Australia/Sydney")


    if not type(max_hour)==pd._libs.tslibs.timestamps.Timestamp:
        raise ValueError(
            f"Could not retrieve date value from binance. Value retrieved: {max_hour} of type {type(max_hour)}")

    assert max_hour < (datetime.datetime.now(tz) + datetime.timedelta(hours=1)), (
        f"Expected a more recent date: {max_hour=}")
    
    return max_hour

