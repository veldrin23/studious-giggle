import pandas as pd


def forward_fill_missing_dates(df):


    df.index = pd.to_datetime(df.index)
    min_date = min(df.index)
    max_date = max(df.index)
        
    date_range = pd.date_range(min_date, max_date, freq='1H')
    
    all_datetimes_df = pd.DataFrame(index = date_range).join(df).ffill()
    

    return all_datetimes_df

