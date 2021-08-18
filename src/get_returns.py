import pandas as pd


def get_returns(conn, live_coins, end_timestamp = None, based_on = "close"):
    cur = conn.cursor()
    
    if end_timestamp is None:
        sql = f"""SELECT * FROM coin_data WHERE TRIM(ticker) IN ('{"','".join(live_coins)}')"""
    else:
        sql = f"""SELECT * FROM coin_data WHERE TRIM(ticker) IN ('{"','".join(live_coins)}') and date <= '{end_timestamp}'"""
    
    coin_data = pd.DataFrame(cur.execute(sql).fetchall())

    assert coin_data.shape[0] > 0, "No data found for timestamp"

    names = list(map(lambda x: x[0].lower(), cur.description))
    coin_data.columns  = names
    
    returns = returns = coin_data.loc[:, ["date", "ticker", "close"]].pivot(columns = ["ticker"], index=["date"]).astype(float).pct_change()[1:].close

    cur.close()

    return returns
    