import pandas as pd


def get_closing_values(conn, live_coins, end_timestamp=None):
    cur = conn.cursor()
    
    if end_timestamp is None:
        sql = f"""SELECT date, ticker, close FROM coin_data WHERE TRIM(ticker) IN ('{"','".join(live_coins)}')"""
    else:
        sql = f"""SELECT date, ticker, close FROM coin_data WHERE TRIM(ticker) IN ('{"','".join(live_coins)}') and date <= '{end_timestamp}'"""
    
    coin_data = pd.DataFrame(cur.execute(sql).fetchall())
    names = list(map(lambda x: x[0].lower(), cur.description))
    coin_data.columns  = names
    

    coin_data["date"] = pd.to_datetime(coin_data["date"])
    coin_data = coin_data.pivot(columns = ["ticker"], index=["date"]).close.astype(float)
    cur.close()
    return coin_data

