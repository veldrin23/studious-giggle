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
    # [print(v) for v in values[:50]]
    # ANDRE, volgende... jy moet die df melt en dan pivot sodat die date and tickers die primary keys is
    # print()
    print(alphas.combined_factors)
    values = alphas.combined_factors["value"]
    [print(v) for v in values]
    alphas.combined_factors["date"] = alphas.combined_factors["date"].astype("str")
    insert_dataframe(alpha_conn, "alpha_factors", alphas.combined_factors, primary_key=["date", "factor", "symbol"])


def main():

    # for testing, only use past 16 days' of data
    n_days_ago = (datetime.datetime.today() + datetime.timedelta(days=-16)).strftime("%Y-%m-%d %H:%M:%S")
    alpha_conn = sqlite3.connect("./data/alpha_factor_values.db")

    coin_conn = sqlite3.connect("./data/coin_data.db")
    cur = coin_conn.cursor()
    cur.execute("select datetime(open_time/1000, 'unixepoch') as date, * from price_history where datetime(open_time/1000, 'unixepoch') > '2021-12-25';")
    price_history = pd.DataFrame(cur.fetchall(), columns = list(map(lambda x: x[0].lower(), cur.description)))
    price_history.date = pd.to_datetime(price_history.date)

    funding_conn = sqlite3.connect("./data/funding_ratios.db")
    cur = funding_conn.cursor()
    cur.execute("""select date, symbol, mean_bids, mean_asks from (select strftime('%Y-%m-%d %H:00:00', date) as date, replace(symbol, "USDT", "") as symbol, mean_bids, mean_asks, rank() over (PARTITION BY strftime('%Y-%m-%d %H:00:00', date), replace(symbol, "USDT", "") order by date asc) val_rank from mean_bid_asks where date > '2021-12-25' and strftime('%M', date) < '05') where val_rank = 1;""")
    fund_ratio = pd.DataFrame(cur.fetchall(), columns = list(map(lambda x: x[0].lower(), cur.description)))
    fund_ratio.date = pd.to_datetime(fund_ratio.date)


    coin_data = fund_ratio.merge(price_history, how = "right")
    coin_data["date"] = pd.to_datetime(coin_data["date"])

    for c in coin_data.columns:
        if c not in ["symbol", "date"]:
            coin_data[c] = coin_data[c].astype(float)

    
    max_coin_date = pd.to_datetime(max(coin_data.index.get_level_values(0)))

    # print(coin_data)
    try:
        sql = "select max(date) from alpha_factors"
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

