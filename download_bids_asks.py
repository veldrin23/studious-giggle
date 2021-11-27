#!/usr/bin/env python3
import asyncio
from binance import AsyncClient, DepthCacheManager, BinanceSocketManager
import pandas as pd
import sqlite3
import sys
from src.db_tools import create_table, insert_dataframe, get_latest_date


def download_and_store_ask_and_bids(depth_cache, conn, symbol, n = 100):    
    time_stamp = depth_cache.update_time
    ask_price, ask_units = list(zip(*depth_cache.get_asks()[:n]))
    bid_price, bid_units = list(zip(*depth_cache.get_bids()[:n]))

    df_out = pd.DataFrame({
        "symbol": symbol,
        "time_stamp": time_stamp,
        "ask_price": ask_price,
        "ask_units": ask_units,
        "bid_price": bid_price,
        "bid_units": bid_units})

    

    insert_dataframe(conn, df=df_out, table_name="bid_asks", primary_key=['symbol', "time_stamp", "ask_price", "bid_price"])
    sys.stdout.write(".")


async def main(n=100):
    conn = sqlite3.connect("./data/crypto.db")
    client = await AsyncClient.create()

    async with DepthCacheManager(client, symbol=f'BTCUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'BTCUSDT', n)

    async with DepthCacheManager(client, symbol=f'ETHUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ETHUSDT', n)

    async with DepthCacheManager(client, symbol=f'DOGEUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'DOGEUSDT', n)

    async with DepthCacheManager(client, symbol=f'ETCUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ETCUSDT', n)

    async with DepthCacheManager(client, symbol=f'XRPUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'XRPUSDT', n)

    async with DepthCacheManager(client, symbol=f'ADAUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ADAUSDT', n)

    async with DepthCacheManager(client, symbol=f'BCHUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'BCHUSDT', n)

    async with DepthCacheManager(client, symbol=f'LTCUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'LTCUSDT', n)

    async with DepthCacheManager(client, symbol=f'DOTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'DOTUSDT', n)

    async with DepthCacheManager(client, symbol=f'EOSUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'EOSUSDT', n)

    async with DepthCacheManager(client, symbol=f'VETUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'VETUSDT', n)

    async with DepthCacheManager(client, symbol=f'AXSUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'AXSUSDT', n)

    async with DepthCacheManager(client, symbol=f'TRXUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'TRXUSDT', n)

    async with DepthCacheManager(client, symbol=f'FILUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'FILUSDT', n)

    async with DepthCacheManager(client, symbol=f'MATICUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'MATICUSDT', n)

    async with DepthCacheManager(client, symbol=f'XLMUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'XLMUSDT', n)

    async with DepthCacheManager(client, symbol=f'UNIUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'UNIUSDT', n)

    async with DepthCacheManager(client, symbol=f'BTTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'BTTUSDT', n)

    async with DepthCacheManager(client, symbol=f'NEOUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'NEOUSDT', n)

    async with DepthCacheManager(client, symbol=f'SOLUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'SOLUSDT', n)

    async with DepthCacheManager(client, symbol=f'LUNAUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'LUNAUSDT', n)

    async with DepthCacheManager(client, symbol=f'AAVEUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'AAVEUSDT', n)

    async with DepthCacheManager(client, symbol=f'CAKEUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'CAKEUSDT', n)

    async with DepthCacheManager(client, symbol=f'THETAUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'THETAUSDT', n)

    async with DepthCacheManager(client, symbol=f'COMPUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'COMPUSDT', n)

    async with DepthCacheManager(client, symbol=f'ZECUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ZECUSDT', n)

    async with DepthCacheManager(client, symbol=f'CHZUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'CHZUSDT', n)

    async with DepthCacheManager(client, symbol=f'SUSHIUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'SUSHIUSDT', n)

    async with DepthCacheManager(client, symbol=f'YFIUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'YFIUSDT', n)

    async with DepthCacheManager(client, symbol=f'ATOMUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ATOMUSDT', n)

    async with DepthCacheManager(client, symbol=f'ZRXUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ZRXUSDT', n)

    async with DepthCacheManager(client, symbol=f'ONTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ONTUSDT', n)

    async with DepthCacheManager(client, symbol=f'HOTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'HOTUSDT', n)

    async with DepthCacheManager(client, symbol=f'SNXUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'SNXUSDT', n)

    async with DepthCacheManager(client, symbol=f'MKRUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'MKRUSDT', n)

    async with DepthCacheManager(client, symbol=f'KSMUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'KSMUSDT', n)

    async with DepthCacheManager(client, symbol=f'GRTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'GRTUSDT', n)

    async with DepthCacheManager(client, symbol=f'XTZUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'XTZUSDT', n)

    async with DepthCacheManager(client, symbol=f'MANAUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'MANAUSDT', n)

    async with DepthCacheManager(client, symbol=f'RVNUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'RVNUSDT', n)

    async with DepthCacheManager(client, symbol=f'HBARUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'HBARUSDT', n)

    async with DepthCacheManager(client, symbol=f'RUNEUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'RUNEUSDT', n)

    async with DepthCacheManager(client, symbol=f'ALGOUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ALGOUSDT', n)

    async with DepthCacheManager(client, symbol=f'XEMUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'XEMUSDT', n)

    async with DepthCacheManager(client, symbol=f'ZILUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ZILUSDT', n)

    async with DepthCacheManager(client, symbol=f'AVAXUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'AVAXUSDT', n)

    async with DepthCacheManager(client, symbol=f'WAVESUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'WAVESUSDT', n)

    async with DepthCacheManager(client, symbol=f'FTTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'FTTUSDT', n)

    async with DepthCacheManager(client, symbol=f'NEARUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'NEARUSDT', n)

    async with DepthCacheManager(client, symbol=f'RVNUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'RVNUSDT', n)

    async with DepthCacheManager(client, symbol=f'ZILUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ZILUSDT', n)

    async with DepthCacheManager(client, symbol=f'BATUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'BATUSDT', n)

    async with DepthCacheManager(client, symbol=f'QTUMUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'QTUMUSDT', n)

    async with DepthCacheManager(client, symbol=f'ONEUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ONEUSDT', n)

    async with DepthCacheManager(client, symbol=f'BNTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'BNTUSDT', n)

    async with DepthCacheManager(client, symbol=f'CELOUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'CELOUSDT', n)

    async with DepthCacheManager(client, symbol=f'SCUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'SCUSDT', n)

    async with DepthCacheManager(client, symbol=f'DGBUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'DGBUSDT', n)

    async with DepthCacheManager(client, symbol=f'ONTUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ONTUSDT', n)

    async with DepthCacheManager(client, symbol=f'ZRXUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ZRXUSDT', n)

    async with DepthCacheManager(client, symbol=f'ZENUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ZENUSDT', n)

    async with DepthCacheManager(client, symbol=f'ANKRUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ANKRUSDT', n)

    async with DepthCacheManager(client, symbol=f'ICXUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'ICXUSDT', n)

    async with DepthCacheManager(client, symbol=f'CRVUSDT') as dcm_socket:
            depth_cache = await dcm_socket.recv()
            download_and_store_ask_and_bids(depth_cache, conn, 'CRVUSDT', n)
        
    await client.close_connection()


if __name__ == "__main__":

    

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
