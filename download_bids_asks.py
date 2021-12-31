#!/usr/bin/env python3
import asyncio
from binance import AsyncClient, DepthCacheManager
import pandas as pd
import sqlite3
import sys
from src.db_tools import insert_dataframe
import numpy as np
from glob import glob
import os
from datetime import datetime
import pytz

def save_mean_bid_asks(conn, bid_asks):
    df = bid_asks.copy()

    weigthed_bids = df[df.bid_price * df.bid_units < np.nanquantile(df.bid_price * df.bid_units, .99)].groupby(["date", "symbol"]).apply(lambda x: np.nansum(x.bid_units * x.bid_price)/np.nansum(x.bid_units)).reset_index()
    weigthed_asks = df[df.ask_price * df.ask_units < np.nanquantile(df.ask_price * df.ask_units, .99)].groupby(["date", "symbol"]).apply(lambda x: np.nansum(x.ask_units * x.ask_price)/np.nansum(x.ask_units)).reset_index()

    weigthed_bids.columns = ["date", "symbol", "mean_bids"]
    weigthed_asks.columns = ["date", "symbol", "mean_asks"]
    out = weigthed_bids.merge(weigthed_asks)

    insert_dataframe(conn, df=out, table_name="mean_bid_asks", primary_key=['date', "symbol"])

async def download_ticker_info(coin, conn, n):

    def fill_series_with_na(s, n):
        s = list(s)
        s += [np.nan] * (n - (len(s)))
        return s

    client = await AsyncClient.create()

    async with DepthCacheManager(client, symbol=coin) as dcm_socket:
        depth_cache = await dcm_socket.recv()
        

        timestamp = depth_cache.update_time
        asks = depth_cache.get_asks()
        bids = depth_cache.get_bids()
        ask_price, ask_units = list(zip(*asks[:n]))
        bid_price, bid_units = list(zip(*bids[:n]))

        df_out = pd.DataFrame({
        "symbol": coin,
        "symbol_index": range(n),
        "timestamp": timestamp,
        "date": str(datetime.fromtimestamp(timestamp/1000).astimezone(pytz.timezone("Australia/Sydney"))),
        "ask_price": fill_series_with_na(ask_price, n),
        "ask_units": fill_series_with_na(ask_units, n),
        "bid_price": fill_series_with_na(bid_price, n),
        "bid_units": fill_series_with_na(bid_units, n)})

        

        insert_dataframe(conn, df=df_out, table_name="bid_asks", primary_key=['symbol', "timestamp", "symbol_index"])
        save_mean_bid_asks(conn, df_out)

if __name__ == "__main__":
    with open("./config/tickers.txt") as _:

        coins = _.read().splitlines()

    loop = asyncio.get_event_loop()
    conn = sqlite3.connect("./data/funding_ratios.db")

    tasks = [loop.create_task(download_ticker_info(coin, conn, n=500)) for coin in coins] 


    loop.run_until_complete(asyncio.wait(tasks))

