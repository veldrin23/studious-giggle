import pandas as pd


def get_aggregated_alpha_factors(conn, live_coins, end_timestamp = None):
    cur = conn.cursor()
    
    if end_timestamp is None:
        sql = f"""
        SELECT  
                date,
                TRIM(ticker) AS ticker, 
                agg_alpha_score 
        FROM    
                temporal_alpha_factors 
        WHERE 
                date IN (SELECT MAX(date) FROM temporal_alpha_factors)
                AND TRIM(ticker) IN ('{"','".join(live_coins)}')"""
    else:
        sql = f"""
        SELECT 
                date,
                TRIM(ticker) AS ticker, 
                agg_alpha_score 
        FROM 
                temporal_alpha_factors 
        where 
                date <='{end_timestamp}' 
            AND TRIM(ticker) IN ('{"','".join(live_coins)}')"""
        
    out = pd.DataFrame(cur.execute(sql).fetchall())

    if out.shape[0] == 0:
        raise ValueError("No alpha factors for timestamp")


    names = list(map(lambda x: x[0].lower(), cur.description))
    out.columns = names
    out["date"] = pd.to_datetime(out["date"])
    
    out = out.pivot(columns = ["ticker"], index=["date"]).agg_alpha_score.astype(float).dropna(axis='rows')
    

    cur.close()
    return out
