def backtesting(conn, live_coins, starting_value=1000, risk_cap=.1, weights_max = .45, start_timestamp=None, end_timestamp=None, equal_weights_testing=False):
    
    
    if end_timestamp is None:
        end_timestamp = get_max_coin_data_date(conn)

    start_timestamp, end_timestamp = pd.to_datetime(start_timestamp), pd.to_datetime(end_timestamp)
    

    
    closing_values = get_closing_values(conn, live_coins, end_timestamp)
    closing_values = forward_fill_missing_dates(closing_values)
    
    returns = get_returns(conn, live_coins, end_timestamp = end_timestamp)
    returns = forward_fill_missing_dates(returns)
    
    alpha_vector = get_aggregated_alpha_factors(conn, live_coins, end_timestamp = end_timestamp)
    alpha_vector = forward_fill_missing_dates(alpha_vector)
    
    
    if start_timestamp is None:
        start_timestamp = min(alpha_vector["date"])
    
    datetime_range = pd.date_range(start_timestamp, end_timestamp, freq="1H")   
    
    
    first_step = True
    for timestamp in tqdm(datetime_range):

        
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
