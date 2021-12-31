#!/usr/bin/env python3

import asyncio
from binance import AsyncClient
import sqlite3
import re



def insert_or_replace_kline_row(kline_row, cur):

    klines_columns = {"open_time" : "bigint",
    "symbol": "str",
    "open": "float",
    "high": "float",
    "low": "float",
    "close": "float",
    "volume": "float",
    "close_time": "bigint",
    "quote_asset_volume": "float",
    "no_of_trades": "int",
    "taker_buy_base_asset_volume": "float",
    "taker_buy_quote_asset_volume": "float",
    "ignore": "float"}


    sql = f"""insert or replace into price_history ({",".join(klines_columns)}) values ({",".join(["?"]*len(klines_columns))})"""

    try: 
        cur.execute(sql, kline_row)

    except sqlite3.OperationalError as e:
        if re.match("no such table", e.args[0]):
            cur.execute(f"""CREATE TABLE price_history (
        {", ".join([f"'{k}' {v} " for k, v in klines_columns.items()])}, 
        PRIMARY KEY ({", ".join(["open_time", "symbol"])})
        );""")
        
        cur.execute(sql, kline_row)



async def download_save_coin_data(conn, symbol):
    
    async_client = await AsyncClient.create()

    async for kline in await async_client.get_historical_klines_generator(symbol, AsyncClient.KLINE_INTERVAL_1HOUR, "30 day ago UTC"):
        to_write = kline[:1] + [symbol] + kline[1:]
        insert_or_replace_kline_row(to_write, conn.cursor())

    await async_client.close_connection()
    
    conn.commit()



if __name__ == "__main__":
    coin_conn = sqlite3.connect("./data/coin_data.db")

    with open("./config/tickers.txt") as _:
        coins = _.read().splitlines()
        
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(download_save_coin_data(coin_conn, symbol)) for symbol in coins] 


    loop.run_until_complete(asyncio.wait(tasks))
