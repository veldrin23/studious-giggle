import sqlite3
import pandas as pd
from tqdm import tqdm
import sys
from binance.client import Client
def extract_updload_funding_rate(client, conn):
    cursor = conn.cursor()
    funding_rates_extract = client.futures_funding_rate()
    timestamp = funding_rates_extract[0]["fundingTime"]
    funding_rate_df = pd.DataFrame({f["symbol"]:float(f["fundingRate"]) for f in funding_rates_extract}.items(), columns = ["symbol", "funding_rate"])
    funding_rate_df["timestamp"] = timestamp

    for i, r in funding_rate_df.iterrows():
        sys.stdout.write(".")
        #print(f"""INSERT INTO FUNDING_RATES (timestamp, symbol, funding_rate) values ({r["timestamp"]}, "{r["symbol"]}", {r["funding_rate"]})""")
        cursor.execute(f"""INSERT INTO FUNDING_RATES (timestamp, symbol, funding_rate) values ({r["timestamp"]}, "{r["symbol"]}", {r["funding_rate"]})""")
if __name__ == "__main__":
    conn = sqlite3.connect("./data/funding_rates.db")
    cur = conn.cursor()
    with open("./src/binance_secret_key.txt") as _:
        api_secret = _.read().splitlines()[0]
    with open("./src/binance_api_key.txt") as _:
        api_key = _.read().splitlines()[0]
    binance_client = Client(api_key, api_secret)
    if len(cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='FUNDING_RATES'").fetchall()) == 0: 
        cur.execute("CREATE TABLE FUNDING_RATES (timestamp int, symbol TEXT, funding_rate float, PRIMARY KEY (timestamp, symbol))")
    extract_updload_funding_rate(binance_client, conn)
