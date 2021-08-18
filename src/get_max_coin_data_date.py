import pandas as pd

def get_max_coin_data_date(conn):
    cur = conn.cursor()
    

    sql = f"""SELECT max(date) as max_date FROM coin_data"""

    
    max_date_df = pd.DataFrame(cur.execute(sql).fetchall())

    cur.close()

    return max_date_df[0][0]
    