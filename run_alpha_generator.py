from src.alpha_factors import GenerateAlphas
import sqlite3
import pandas as pd
from src.db_tools import insert_dataframe
import datetime
import pickle

def run_alpha_factors_and_upload(coin_data, conn):
    alphas = GenerateAlphas(coin_data)
    alphas.run_factors("all")



    # ANDRE, volgende... jy moet die df melt en dan pivot sodat die date and tickers die primary keys is
    # print()
    alphas.combined_factors["date"] = alphas.combined_factors["date"].astype("str")
    insert_dataframe(conn, "alpha_factors", alphas.combined_factors, primary_key=["date", "factor", "ticker"])


def main():

    # for testing, only use past 2 days' of data
    eight_days_ago = (datetime.datetime.today() + datetime.timedelta(days=-8)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect("./data/crypto.db")

    sql = f"select * from coin_data where date >= '{eight_days_ago}';"
    cur = conn.cursor()
    cur.execute(sql)

    # latest value upload
    coin_data = pd.DataFrame(cur.fetchall())

    names = list(map(lambda x: x[0].lower(), cur.description))

    coin_data.columns  = names


    coin_data["date"] = pd.to_datetime(coin_data["date"])
    max_coin_date = pd.to_datetime(max(coin_data["date"]))
    
    coin_data.set_index(["date", "ticker"], inplace=True)
    

    try:
        sql = "select max(date) from alpha_factors"
        max_alpha_date = pd.to_datetime(cur.execute(sql).fetchall()[0][0])

        if max_coin_date > max_alpha_date:
            print("Alpha factors outdated, running alpha factors")
            run_alpha_factors_and_upload(coin_data, conn)

        elif max_coin_date == max_alpha_date:
            print("Alpha factors up to date")

        else:
            print("Somehow alpha factors are ahead of coin data?")

    except Exception as err:
        print(f"Table probably doesn't exist, error: {str(err)}\nRunning alpha factors from the start (This will take a while)")

        sql = f"select * from coin_data where date;"
        cur = conn.cursor()
        cur.execute(sql)
        coin_data = pd.DataFrame(cur.fetchall())
        names = list(map(lambda x: x[0].lower(), cur.description))
        coin_data.columns  = names

        coin_data["date"] = pd.to_datetime(coin_data["date"])
        
        coin_data.set_index(["date", "ticker"], inplace=True)
        
        run_alpha_factors_and_upload(coin_data, conn)




if __name__ == "__main__":
    main()

