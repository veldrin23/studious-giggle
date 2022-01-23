import pandas as pd


def get_closing_values(conn, live_coins, end_timestamp=None):
    cur = conn.cursor()
    
    if False: ## TODO: fix dit, lui drol
        sql = f"""
        SELECT 
                datetime(open_time/1000, 'unixepoch', 'localtime') as date, 
                symbol, 
                close 
        FROM 
                price_history 
        WHERE 
                1=1
                AND TRIM(symbol) IN ('{"','".join(live_coins)}')"""
    else:
        sql = f"""
        SELECT 
                datetime(open_time/1000, 'unixepoch', 'localtime') as date, 
                symbol, 
                close 
        FROM   
                price_history
        WHERE 
                1=1 
                AND TRIM(symbol) IN ('{"','".join(live_coins)}') 
                AND datetime(open_time/1000, 'unixepoch', 'localtime') <= '{end_timestamp}'"""
    
    coin_data = pd.DataFrame(cur.execute(sql).fetchall())
    names = list(map(lambda x: x[0].lower(), cur.description))
    coin_data.columns  = names
    

    coin_data["date"] = pd.to_datetime(coin_data["date"])
    
    if pd.__version__ == "0.20.3":
        coin_data = coin_data.set_index("date").pivot(columns="symbol").close.astype(float)
    else:
        coin_data = coin_data.pivot(columns = ["symbol"], index=["date"]).close.astype(float)
    coin_data = coin_data.ffill().dropna(axis="rows")
    cur.close()
    return coin_data

