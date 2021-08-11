import sqlite3
import pandas as pd
from tqdm import tqdm
import sys


def create_df_field_types_dict(df):
       
    # remapping data types to what's allowed in sqlite
    type_mapping = {"object": "text"}
    
    def replace_type(type_dict, field_type):
        field_type = str(field_type)
        try:
            return type_dict[field_type]
        except KeyError as err:
            return field_type
        

    out = {}
    for k, v in df.dtypes.items():
        out[k] = replace_type(type_mapping, v)
    
    # change date field to date type
    out["date"] = "date"
    return out
    

def create_table(conn: sqlite3.Connection, 
                 table_name: str, 
                 fields: dict, 
                 primary_key: [str]) -> None:
    
    sql = f"""
        CREATE TABLE {table_name} (
        {", ".join([f"'{k}' {v} " for k, v in fields.items()])}, 

        PRIMARY KEY ({", ".join(primary_key)})
        );

        """

    cur = conn.cursor()
    
    try:
        cur.execute(sql)
        print(f"Table {table_name} created")
    except Exception as err:
        print(err)
    print("table created")
    cur.close()


def insert_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame, create_if_dont_exist=True, primary_key = ["date", "ticker"]):
    cur = conn.cursor()
    if create_if_dont_exist:
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        cur.execute(sql)
        if cur.fetchall()==[]:
            print("Creating table")
            df_field_types = create_df_field_types_dict(df)
            create_table(conn, table_name, df_field_types, primary_key=primary_key)

    inserted = 0
    for i, r in df.iterrows():
        sql = f"""INSERT OR REPLACE INTO {table_name} ({",".join(df.columns)})  
                    VALUES ({",".join(["?"]*df.shape[1])});
        """

        
        task = r.values

        try:
            cur.execute(sql, task)
            
            inserted +=1
        except Exception as err:
        	print(err)

    conn.commit()
    cur.close()
    print(f"\t{inserted} rows inserted")
        


def get_latest_date(conn, table_name):
	cur = conn.cursor()

	sql = f"""select max(date)  as max_date from {table_name} limit 1;"""

	return cur.execute(sql).fetchall()



def get_aggregated_alpha_factors(conn, live_coins, timestamp = None):

    """
    Get the alpha aggregators at a specified timestamp

    Parameters
    ----------
    conn : sqlite3 connection object
        Connection to sqlite3 database containing the aggregated alpha factors
    live_coins : list of str values
        List of coins to be considered in the live portfolio
    timestamp: str date value (%Y-%m-%d %H:%M:%S)
        timestamp of when the aggregated values should be retrieved. If no time is specified, the latest values will be retrieved 

    Returns
    -------
    out : pandas dataframe
        alpha factors for each of the live coins
        """
    cur = conn.cursor()
    
    if date is None:
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
                date ='{timestamp}' 
            AND TRIM(ticker) IN ('{"','".join(live_coins)}')"""
        
    out = pd.DataFrame(cur.execute(sql).fetchall())
    names = list(map(lambda x: x[0].lower(), cur.description))
    out.columns = names
    cur.close()
    return out


def get_returns(conn, live_coins, end_timestamp = None, based_on = "close"):
    """
    Get the returns baed on closing values (by default)

    Parameters
    ----------
    conn : sqlite3 connection object
        Connection to sqlite3 database containing the aggregated alpha factors
    live_coins : list of str values
        List of coins to be considered in the live portfolio
    end_timestamp: str date value (%Y-%m-%d %H:%M:%S)
        last timestamp of the return values to be retreived 

    Returns
    -------
    returns: pandas dataframe
        dataframe containign the percentage change from one timestamp to the next 

    """

    cur = conn.cursor()
    
    if end_timestamp is None:
        sql = f"""SELECT * FROM coin_data WHERE TRIM(ticker) IN ('{"','".join(live_coins)}')"""
    else:
        sql = f"""SELECT * FROM coin_data WHERE TRIM(ticker) IN ('{"','".join(live_coins)}') and date <= '{end_timestamp}'"""
    
    coin_data = pd.DataFrame(cur.execute(sql).fetchall())
    names = list(map(lambda x: x[0].lower(), cur.description))
    coin_data.columns  = names
    
    returns = coin_data.set_index(["date", "ticker"]).loc[:, [based_on]].reset_index().pivot(columns = ["ticker"], index=["date"]).astype(float).pct_change()[1:][based_on]
    cur.close()
    return returns
    
