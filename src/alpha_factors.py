import numpy as np
from functools import lru_cache


def factor_001(coin_data):
    """
    Alpha#1: (rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5) 
    where       ts_argmax(x, d) = which day ts_max(x, d) occurred on
    and         signedpower(x, a) = x^a 

    """
    returns = coin_data.loc[:,["returns"]].reset_index().pivot(index="date", columns="ticker")
    returns[returns < 0] = returns.rolling(window=20).std(skipna=False)
    ts_argmax = np.square(returns).rolling(window=5).apply(np.argmax) + 1
    factor = ts_argmax.rank(axis='columns', pct=True) - 0.5
    return factor


def factor_002(coin_data):
    """
    Alpha#2: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
    """
    volume = coin_data.loc[:,["volume"]].reset_index().pivot(index="date", columns="ticker").volume
    open_ = coin_data.loc[:,["open"]].reset_index().pivot(index="date", columns="ticker").open
    close = coin_data.loc[:,["close"]].reset_index().pivot(index="date", columns="ticker").close
    
    temp_1 = np.log(volume).diff(2)
    temp_1_ranked = temp_1.rank(axis="columns", pct=True)
    
    temp_2 = (close - open_)/open_
    temp_2_ranked = temp_2.rank(axis="columns", pct=True)
    
    factor = -1 * temp_1_ranked.rolling(window=6).corr(temp_2_ranked)
    
    return factor



def factor_003(coin_data):
    """
    Alpha#3: (-1 * correlation(rank(open), rank(volume), 10))
    """
    open_ = coin_data.loc[:,["open"]].reset_index().pivot(index="date", columns="ticker").open
    volume = coin_data.loc[:,["volume"]].reset_index().pivot(index="date", columns="ticker").volume
    
    ranked_volume = volume.rank(axis="rows")
    ranked_open = open_.rank(axis="rows")
    
    factor = -1 * ranked_volume.rolling(window=10).corr(ranked_open)
    
    return factor
