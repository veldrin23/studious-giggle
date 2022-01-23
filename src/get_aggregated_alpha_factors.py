import pandas as pd


def get_aggregated_alpha_factors(conn, live_coins, start_time = None, end_timestamp = None):
    cur = conn.cursor()
    
    if False: ## TODO: fix dit, lui gat
        sql = f"""
        SELECT  
                date,
                TRIM(symbol) AS symbol, 
                agg_alpha_score 
        FROM    
                temporal_alpha_factors 
        WHERE 
                date IN (SELECT MAX(date) FROM temporal_alpha_factors)
                AND TRIM(symbol) IN ('{"','".join(live_coins)}')"""
    else:
        sql = f"""
        SELECT 
                date,
                TRIM(symbol) AS symbol, 
                agg_alpha_score 
        FROM 
                temporal_alpha_factors 
        where 
                date <='{end_timestamp}' 
            AND date >='{start_time}' 
            AND TRIM(symbol) IN ('{"','".join(live_coins)}')"""

    out = pd.DataFrame(cur.execute(sql).fetchall())

    if out.shape[0] == 0:
        raise ValueError("No alpha factors for timestamp")


    names = list(map(lambda x: x[0].lower(), cur.description))
    out.columns = names
    out["date"] = pd.to_datetime(out["date"])
    out.to_csv("out_1.csv")
    out = out.pivot(columns = ["symbol"], index=["date"]).agg_alpha_score.astype(float).dropna(axis='rows')
    out.to_csv("out_2.csv")

    cur.close()
    return out
