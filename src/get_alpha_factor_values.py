import pandas as pd


def get_alpha_factor_values(conn, live_coins, live_factors, end_timestamp=None):
    cur = conn.cursor()
    
    if end_timestamp is None:
        sql = f"""SELECT date, ticker, factor, value FROM alpha_factors WHERE TRIM(factor) IN ('{"','".join(live_factors)}') AND TRIM(ticker) IN ('{"','".join(live_coins)}')"""
    else:
        sql = f"""SELECT date, ticker, factor, value FROM alpha_factors WHERE TRIM(factor) IN ('{"','".join(live_factors)}') and date <= '{end_timestamp}' AND TRIM(ticker) IN ('{"','".join(live_coins)}')"""
    
    
    factor_data = pd.DataFrame(cur.execute(sql).fetchall())

    names = list(map(lambda x: x[0].lower(), cur.description))

    factor_data.columns  = names
    
    
    factor_data["date"] = pd.to_datetime(factor_data["date"])
    if pd.__version__ == "0.20.3":
        factor_data = factor_data.set_index(["date", "ticker"]).pivot(columns="factor").value.astype(float)
    else:
        factor_data = factor_data.pivot(index = ["date", "ticker"], columns=["factor"]).value.astype(float)
    cur.close()
    
    return factor_data