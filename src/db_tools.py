import sqlite3
import pandas as pd
from tqdm import tqdm


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
        {", ".join([f"{k} {v} NOT NULL" for k, v in fields.items()])}, 

        PRIMARY KEY ({", ".join(primary_key)})
        );

        """

    cur = conn.cursor()
    
    try:
        cur.execute(sql)
        print(f"Table {table_name} created")
    except Exception as err:
        print(err)

    cur.close()


def insert_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame, create_if_dont_exist=True):
    cur = conn.cursor()
    if create_if_dont_exist:
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        cur.execute(sql)
        if cur.fetchall()==[]:
            df_field_types = create_df_field_types_dict(df)
            create_table(conn, table_name, df_field_types, primary_key=["date", "ticker"])

    inserted = 0
    for i, r in df.iterrows():
        sql = f"""INSERT OR REPLACE INTO {table_name}(date, ticker, open, high, low, close, volume_1, volume_2, trade_count, unknown_1, unknown_2, unknown_3 ,unknown_4)  
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);
        """
        
        task = r.values

        try:
            cur.execute(sql, task)
            
            inserted +=1
        except Exception as err:
        	print(err)
            # print(str(err) == f"no such table: {table_name}")
    conn.commit()
    # print(f"Inserted {inserted} rows into {table_name}")
    cur.close()
        


def get_latest_date(conn, table_name):
	cur = conn.cursor()

	sql = f"""select max(date)  as max_date from {table_name} limit 10;"""

	return cur.execute(sql).fetchall()