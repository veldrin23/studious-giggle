import pandas as pd

def get_min_coin_data_date(conn):
    cur = conn.cursor()
    

    sql = f"""SELECT min(date) as min_date FROM coin_data"""

    
    min_date_df = pd.DataFrame(cur.execute(sql).fetchall())

    cur.close()

    return min_date_df[0][0]
    