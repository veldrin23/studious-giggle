#!/usr/bin/env python3

from src.alpha_factors import GenerateAlphas
import sqlite3
import pandas as pd
from src.db_tools import insert_dataframe
import datetime
import pickle

pd.options.display.max_rows = 100
pd.options.display.max_columns = 500

def run_alpha_factors_and_upload(coin_data, alpha_conn):
    alphas = GenerateAlphas(coin_data)
    alphas.run_factors("all")
    # values = alphas.combined_factors.value
    # ANDRE, volgende... jy moet die df melt en dan pivot sodat die date and tickers die primary keys is

    alphas.combined_factors["date"] = alphas.combined_factors["date"].astype("str")
    insert_dataframe(alpha_conn, "alpha_factors", alphas.combined_factors, primary_key=["date", "factor", "symbol"])


def main():

    with open('./config/live_symbols.txt') as _:
        live_symbols = _.read().splitlines()
        
    alpha_conn = sqlite3.connect("./data/alpha_factor_values.db")
    coin_conn = sqlite3.connect("./data/coin_data.db")
    funding_conn = sqlite3.connect("./data/funding_ratios.db")

    cursor_colnames = lambda cur: list(map(lambda x: x[0].lower(), cur.description))

    # get price history of live symbols
    cur = coin_conn.cursor()
    sql = f"""
    select 
        datetime(open_time/1000, 'unixepoch', 'localtime') as date, 
        * 
    from 
        price_history 
    where 
        1 = 1
        and datetime(open_time/1000, 'unixepoch', 'localtime') > ?
        and symbol in ({",".join(["?"]  * len(live_symbols))});
    """
    cur.execute(sql, ('2021-12-25', *live_symbols))
    price_history = pd.DataFrame(cur.fetchall(), columns = cursor_colnames(cur))
    price_history.date = pd.to_datetime(price_history.date)

    # get mean bids and mean asks for live symbols
    cur = funding_conn.cursor()
    sql = f"""
    select 
        strftime('%Y-%m-%d %H:00:00', date, 'localtime')  as date,
        symbol, 
        mean_bids, 
        mean_asks 
    from 
        (
            select 
                date, 
                symbol, 
                mean_bids, 
                mean_asks, 
                rank() over (PARTITION BY strftime('%Y-%m-%d %H:00:00', date, 'localtime'), symbol order by date asc) val_rank 
            from 
                mean_bid_asks 
            where 
                1 = 1
                and date >= ? 
                and strftime('%M', date) < '05' 
                and symbol in ({",".join(["?"]  * len(live_symbols))})
        ) 
    where 
        1 = 1 
        and val_rank = 1;
    """
    cur.execute(sql, ('2022-01-01', *live_symbols))
    bid_asks = pd.DataFrame(cur.fetchall(), columns = cursor_colnames(cur))
    bid_asks.date = pd.to_datetime(bid_asks.date)
    
    coin_data = bid_asks.merge(price_history, how = "right")
    coin_data["date"] = pd.to_datetime(coin_data["date"])

    max_coin_date = max(coin_data["date"])

    try:
        sql = "select max(date) from alpha_factors;"
        cur = alpha_conn.cursor()
        max_alpha_date = pd.to_datetime(cur.execute(sql).fetchall()[0][0])
        if max_coin_date > max_alpha_date:
            print("Alpha factors outdated, running alpha factors")
            run_alpha_factors_and_upload(coin_data, alpha_conn)

        elif max_coin_date == max_alpha_date:
            print("Alpha factors up to date")

        else:
            print("Somehow alpha factors are ahead of coin data?")


    except Exception as err:
        print(f"Table probably doesn't exist, error: {str(err)}\nRunning alpha factors from the start (This will take a while)")
        
        run_alpha_factors_and_upload(coin_data, alpha_conn)




if __name__ == "__main__":
    main()

