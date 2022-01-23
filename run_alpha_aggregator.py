#!/usr/bin/env python3

import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
from src.db_tools import insert_dataframe

def place_in_quantile_groups(x: pd.Series, quantiles: list) -> list:
    """
    returns a list of same length with values places in buckets based on the quantile values. Number of buckets is one more than the number of quntiles

    Arg:
        x: list of numeric values
        quantiles: list of numeric values between 0 and 1  

    """

    assert (min(quantiles) > 0) & (max(quantiles)<1), "Quantile values should be between 0 and 1"

    no_of_groups = len(quantiles) + 1

    
    buckets = np.arange(-(no_of_groups//2), (no_of_groups)//2+1)

    
    if no_of_groups % 2 == 0:
        buckets = np.delete(buckets, np.where(buckets == 0))

    x_copy = x.copy()
    cutoffs = np.quantile(x, quantiles)


    x_copy[x_copy<cutoffs[0]]=buckets[0]
    x_copy[x_copy>=cutoffs[-1]]=buckets[-1]

    for i, a, b in zip(buckets[1:], cutoffs[:-1], cutoffs[1:]):

        x_copy[(a<x_copy) & (x_copy<=b)]=i
        
    return x_copy

    

def score_alpha_factors(alpha_factors: pd.DataFrame, shifted_returns: pd.DataFrame, a:float=0.0, b:float=1.0) -> pd.DataFrame:
    
    def normalise_factor_scores(x: pd.Series) -> pd.Series:
        x_min = min(x)
        x_max = max(x)

        if x_min == x_max:
            return pd.Series([(a+b)/2] * len(x))
        else:
            return (x - x_min)/(x_max - x_min) * (b - a)  + a 
            
    # bin the alpha factors and shifted returns
    binned_shifted_returns = shifted_returns.apply(lambda x: place_in_quantile_groups(x, [.025, .25, .75, .975]), axis=0)

    symbols = binned_shifted_returns.columns
    weights_out = pd.DataFrame(columns = ["symbol", "factor", "score"])
    # calculate the alpha factor weight per symbol

    for t in symbols:
        binned_alpha_factors = alpha_factors.loc[:,[t]][t].dropna().apply(lambda x: place_in_quantile_groups(x, [.5]), axis=0)

        joined_table = binned_shifted_returns.loc[:,[t]].join(binned_alpha_factors).dropna()
        weights_df = pd.DataFrame(columns = ["symbol", "factor", "score"])

        for c in joined_table.columns[1:]:            
            weights_df.loc[weights_df.shape[0]+1, :] = [t, c, sum(joined_table.iloc[:,0] * joined_table[c])]

        weights_df["balanced_score"] = normalise_factor_scores(weights_df["score"])

        weights_out = pd.concat([weights_out, weights_df])
    return weights_out

    

def create_temporal_alpha_values(alpha_factors, alpha_scores):
    symbols = pd.unique([x[0] for x in alpha_factors.columns])
    out = None
    for symbol in tqdm(symbols):
        df1 = alpha_factors[symbol]
        df2 = alpha_scores.loc[:, ["symbol", "factor", "balanced_score"]].set_index(["symbol","factor"]).T[symbol].astype(float).values
        final_alpha_scores = pd.DataFrame((df1*df2).dropna().sum(axis=1).reset_index())
        final_alpha_scores.columns = ["date", "agg_alpha_score"]
        final_alpha_scores["symbol"] = symbol
        
        if out is None:
            out = final_alpha_scores

        else:
            out = pd.concat([out, final_alpha_scores])

    return out



def main(aggregator = "quantile_scoring") -> None:

    if aggregator == "quantile_scoring":
        price_conn = sqlite3.connect("./data/coin_data.db")
        alpha_factor_conn = sqlite3.connect("./data/alpha_factor_values.db")
        cur = price_conn.cursor()

        with open('./config/live_symbols.txt') as _:
            live_symbols = _.read().splitlines()
            
        with open("./config/live_alpha_factors.txt") as _:
            live_alpha_factors = _.read().splitlines()

        cur = price_conn.cursor()
        coin_data = pd.DataFrame(cur.execute(f"""select *, datetime(open_time/1000, 'unixepoch', 'localtime') as date from price_history where symbol in ('{"','".join(live_symbols)}')""").fetchall())

        names = list(map(lambda x: x[0].lower(), cur.description))
        coin_data.columns  = names

        cur = alpha_factor_conn.cursor()
        alpha_factors = pd.DataFrame(cur.execute(f"""select * from alpha_factors where factor  in ('{"','".join(live_alpha_factors)}') and symbol in ('{"','".join(live_symbols)}')""").fetchall())
        names = list(map(lambda x: x[0].lower(), cur.description))
        alpha_factors.columns  = names

        shifted_returns = coin_data.loc[:, ["date", "symbol", "close"]].pivot(index=["date"], columns=["symbol"]).astype(float).pct_change().shift(-1)[:-1].close
        alpha_factors = alpha_factors.pivot(index="date", columns = ["symbol", "factor"]).value.astype(float)

        alpha_scores = score_alpha_factors(alpha_factors, shifted_returns)

        temporal_alpha_factors = create_temporal_alpha_values(alpha_factors, alpha_scores)
        insert_dataframe(alpha_factor_conn, "temporal_alpha_factors", temporal_alpha_factors, primary_key=["date", "symbol"])


if __name__ == "__main__":
    main()
