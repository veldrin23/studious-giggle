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

    

def score_alpha_factors(alpha_factors, shifted_returns):
    
    # bin the alpha factors and shifted returns
    
    binned_shifted_returns = shifted_returns.apply(lambda x: place_in_quantile_groups(x, [.025, .15, .85, .975]), axis=0)
    
    
    tickers = binned_shifted_returns.columns
    weights_out = pd.DataFrame(columns = ["ticker", "factor", "score"])
    # calculate the alpha factor weight per ticker
    for t in tickers:

        binned_alpha_factors = alpha_factors.loc[:,[t]][t].dropna().apply(lambda x: place_in_quantile_groups(x, [.5]), axis=0)

    
        joined_table = binned_shifted_returns.loc[:,[t]].join(binned_alpha_factors).dropna()
        weights_df = pd.DataFrame(columns = ["ticker", "factor", "score"])
        for c in joined_table.columns[1:]:
#             print(c)
            
            weights_df.loc[weights_df.shape[0]+1, :] = [t, c, sum(joined_table.iloc[:,0] * joined_table[c])]
            

        weights_df["balanced_score"] = weights_df["score"] - min(weights_df["score"])
        weights_df["balanced_score"] = weights_df["balanced_score"] / sum(weights_df["balanced_score"])
        weights_out = pd.concat([weights_out, weights_df])
    return weights_out

    

def create_temporal_alpha_values(alpha_factors, alpha_scores):
    tickers = pd.unique([x[0] for x in alpha_factors.columns])
    out = None
    for ticker in tqdm(tickers):
        df1 = alpha_factors[ticker]
        df2 = alpha_scores.loc[:, ["ticker", "factor", "balanced_score"]].set_index(["ticker","factor"]).T[ticker].astype(float).values
        final_alpha_scores = pd.DataFrame((df1*df2).dropna().sum(axis=1).reset_index())
        final_alpha_scores.columns = ["date", "agg_alpha_score"]
        final_alpha_scores["ticker"] = ticker
        
        if out is None:
            out = final_alpha_scores

        else:
            out = pd.concat([out, final_alpha_scores])

    return out



def main(aggregator = "quantile_scoring") -> None:

    if aggregator == "quantile_scoring":
        conn = sqlite3.connect("./data/crypto.db")

        cur = conn.cursor()

        with open('./config/tickers_to_ignore.txt') as f:
            tickers_to_ignore = f.read().split("\n")
            
        with open("./config/alpha_factors_to_ignore.txt") as f:
            alpha_factors_to_ignore = f.read().split("\n")

        coin_data = pd.DataFrame(cur.execute(f"""select * from coin_data where ticker not in ('{"','".join(tickers_to_ignore)}')""").fetchall())
        names = list(map(lambda x: x[0].lower(), cur.description))
        coin_data.columns  = names

        alpha_factors = pd.DataFrame(cur.execute(f"""select * from alpha_factors where factor not in ('{"','".join(alpha_factors_to_ignore)}') and ticker not in ('{"','".join(tickers_to_ignore)}')""").fetchall())
        names = list(map(lambda x: x[0].lower(), cur.description))
        alpha_factors.columns  = names

        shifted_returns = coin_data.loc[:, ["date", "ticker", "close"]].pivot(index=["date"], columns=["ticker"]).astype(float).pct_change().shift(-1)[:-1].close
        alpha_factors = alpha_factors.pivot(index="date", columns = ["ticker", "factor"]).value.astype(float)

        alpha_scores = score_alpha_factors(alpha_factors, shifted_returns)

        temporal_alpha_factors = create_temporal_alpha_values(alpha_factors, alpha_scores)

    insert_dataframe(conn, "temporal_alpha_factors", temporal_alpha_factors, primary_key=["date", "ticker"])


if __name__ == "__main__":
    main()
