from src.alpha_factors import GenerateAlphas
import sqlite3
import pandas as pd




if __name__ == "__main__":
	conn = sqlite3.connect("./data/crypto.db")

	sql = "select * from coin_data;"
	cur = conn.cursor()
	cur.execute(sql)

	# latest value upload
	coin_data = pd.DataFrame(cur.fetchall())

	names = list(map(lambda x: x[0].lower(), cur.description))

	coin_data.columns  = names


	coin_data["date"] = pd.to_datetime(coin_data["date"])
	max_coin_date = max(coin_data["date"])
	
	coin_data.set_index(["date", "ticker"], inplace=True)
	

	# latest alpha factor upload
	try:
		sql = "select max(date) from alpha_data"
		cur.execute(sql).fetchall()

	except sqlite3.OperationalError:
		print("\n\nNo alpha factors created yet\n\n")


		alphas = GenerateAlphas(coin_data)

		alphas.run_factors("all")

		print(alphas.combined_factors.shape)





	# coin_data = coin_data.set_index([coin_data["date"], coin_data["ticker"]]).\
	# drop(columns = ["date", "ticker"]).rename(columns = {"volume_1": "volume"})\
	# .loc[:, ["open", "high","low", "close", "volume", "trade_count"]].astype("float")

	# coin_data["returns"] = coin_data.sort_index(1).groupby("ticker").close.pct_change()

	# coin_data.dropna(axis="rows", inplace=True)

	# coin_data = coin_data.reset_index().groupby(["date", "ticker"]).mean()



