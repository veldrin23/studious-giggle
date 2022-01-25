#!/usr/bin/env python3

from src.create_risk_model import create_risk_model
from src.get_returns import get_returns
from src.forward_fill_missing_dates import forward_fill_missing_dates
from src.get_closing_values import get_closing_values
from src.get_max_coin_data_date import get_max_coin_data_date
from src.get_aggregated_alpha_factors import get_aggregated_alpha_factors
from src.get_optimal_holdings import OptimalHoldings
from tqdm import tqdm
from datetime import timedelta 
import pandas as pd
import sqlite3

def backtesting(coin_conn: sqlite3.Connection, alpha_conn: sqlite3.Connection, live_coins: list, starting_value: float=1000.0, risk_cap:float=.05, weights_max:float= .35, start_timestamp=None, end_timestamp=None, equal_weights_testing=False) -> pd.DataFrame:
    
    
    if end_timestamp is None:
        end_timestamp = get_max_coin_data_date(coin_conn)

    start_timestamp, end_timestamp = pd.to_datetime(start_timestamp), pd.to_datetime(end_timestamp)
    
    closing_values: pd.DataFrame = get_closing_values(coin_conn, live_coins, end_timestamp=end_timestamp)
    closing_values = forward_fill_missing_dates(closing_values)

    returns = closing_values.pct_change().shift(-1)[:-1]


    alpha_vector = get_aggregated_alpha_factors(alpha_conn, live_coins, start_time=start_timestamp, end_timestamp = end_timestamp)
    alpha_vector = forward_fill_missing_dates(alpha_vector)

    
    if start_timestamp is None:
        start_timestamp = min(alpha_vector["date"])
    
    datetime_range = pd.date_range(start_timestamp, end_timestamp, freq="1H")   
    
    
    first_step = True
    for i, timestamp in tqdm(enumerate(datetime_range), total=len(datetime_range)):

        closing_values_ss = closing_values[closing_values.index <= timestamp]
        returns_ss = returns[returns.index <= timestamp]
        alpha_vector_ss = alpha_vector[alpha_vector.index == timestamp].T

        risk_model = create_risk_model(returns_ss)
        
        if equal_weights_testing:
            optimal_weights = pd.DataFrame({"ticker": returns.columns, "weight": [1/returns.shape[1]] * returns.shape[1]})
        else:

            optimal_weights = OptimalHoldings(risk_cap=risk_cap, weights_max=weights_max).find(alpha_vector_ss, risk_model['factor_betas'], risk_model['factor_cov_matrix'], risk_model['idiosyncratic_var_vector'])

            
            optimal_weights.reset_index(inplace=True)
            optimal_weights.columns = ["ticker", "weight"]

        if first_step:

            holdings = starting_value * optimal_weights["weight"]
            holdings_df = pd.DataFrame([[str(timestamp)] + list(holdings)], columns = ["datetime"]  + list(returns.columns))

            first_step = False

        else:
            prev_time_stamp = timestamp + timedelta(hours = -1)

            # growth based on closing prices
            try:
                holdings = holdings * (closing_values_ss[closing_values_ss.index == timestamp].values  / closing_values_ss[closing_values_ss.index == prev_time_stamp].values)[0]
            except IndexError:
                print(timestamp)

            # rebalance portfolio
            if not equal_weights_testing:
                holdings = sum(holdings) * optimal_weights["weight"]

            holdings_df.loc[holdings_df.shape[0], :] = [str(timestamp)] + list(holdings)
    holdings_df.set_index("datetime", inplace=True)
    return holdings_df


if __name__ == "__main__":

    price_conn = sqlite3.connect("./data/coin_data.db")
    alpha_factor_conn = sqlite3.connect("./data/alpha_factor_values.db")

    with open('./config/live_symbols.txt') as _:
        live_symbols = _.read().splitlines()
        
    with open("./config/live_alpha_factors.txt") as _:
        live_alpha_factors = _.read().splitlines()

    alpha_testing = backtesting(
        price_conn, 
        alpha_factor_conn, 
        live_coins=live_symbols, 
        start_timestamp='2022-01-01 03:00:00', 
        end_timestamp='2022-01-23 11:00:00', 
        equal_weights_testing=False)

    alpha_testing.to_csv("./apha_testing.csv")


    equal_weights = backtesting(
        price_conn, 
        alpha_factor_conn, 
        live_coins=live_symbols, 
        start_timestamp='2022-01-01 03:00:00', 
        end_timestamp='2022-01-23 11:00:00', 
        equal_weights_testing=True)

    equal_weights.to_csv("./equal_weights_testing.csv")    