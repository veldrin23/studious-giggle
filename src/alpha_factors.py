
import numpy as np
import pandas as pd

class GenerateAlphas:
#     __refs__ = defaultdict(list)
    def __init__(self, coin_df):
        

        self.coin_data = coin_df[:-1]
        self.close = coin_df.loc[:,["close"]].reset_index().pivot(index="date", columns="ticker").close.astype(float)
        self.open = coin_df.loc[:,["open"]].reset_index().pivot(index="date", columns="ticker").open.astype(float)
        self.volume = coin_df.loc[:,["volume_1"]].reset_index().pivot(index="date", columns="ticker").volume_1.astype(float)
        self.low = coin_df.loc[:,["low"]].reset_index().pivot(index="date", columns="ticker").low.astype(float)
        self.high = coin_df.loc[:,["high"]].reset_index().pivot(index="date", columns="ticker").high.astype(float)
        self.trade_count = coin_df.loc[:,["trade_count"]].reset_index().pivot(index="date", columns="ticker").trade_count.astype(float)

        self.returns = self.close.pct_change()

        self.combined_factors = None
        
    def rename_factor_columns(self, factor, coin_data, factor_no):
#         columns=[(x, f"factor_{factor_no}") for x in pd.unique(coin_data.index.get_level_values(1))]
        # print("aaa")
        # print(coin_data.head(5))
        columns=[(f"factor_{factor_no}", x) for x in pd.unique(coin_data.index.get_level_values(1))]
        factor.columns=pd.MultiIndex.from_tuples(columns)
        return factor

    
    extract_fields = lambda coin_data, fields: coin_data.loc[:,field].reset_index().pivot(index="date", columns="ticker").loc[:,field]
    
    rolling_rank = lambda data: data.size - data.argsort().argsort()[-1]



    def run_factors(self, factors_to_run = "all"):
        af_dict = {
            "factor_001": self.generate_factor_001,
            "factor_002": self.generate_factor_002,
            "factor_003": self.generate_factor_003,
            "factor_004": self.generate_factor_004,
            "factor_nvp": self.generate_factor_nvp
        }
        
        if factors_to_run == "all":
            for key, value in af_dict.items():
                print(f"Running {key}")
                value()
        
        else:
            if type(factors_to_run) == list:
                for factor_to_run in factors_to_run:
                    print(f"Running {factor_to_run}")
                    af_dict["factor_"+ factor_to_run]()
            else:
                print(f"Running {factors_to_run}")
                af_dict["factor_"+ factors_to_run]()
    

                
                
    def merge_factor(self, factor):
        if self.combined_factors is None:
            self.combined_factors = factor
        else:
            self.combined_factors = self.combined_factors.join(factor)
        
        
        
    
    def generate_factor_001(self):
        """
        Alpha#1: (rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5) 
        where       ts_argmax(x, d) = which day ts_max(x, d) occurred on
        and         signedpower(x, a) = x^a 
        """
        # print("aa")
        returns_temp = self.returns.copy()
        returns_temp[returns_temp < 0] = returns_temp.rolling(window=20).std(skipna=False)
        ts_argmax = np.square(returns_temp).rolling(window=5).apply(np.argmax) + 1
        factor = ts_argmax.rank(axis='rows', pct=True) - 0.5

        factor = self.rename_factor_columns(factor, self.coin_data, "001")#.dropna().apply(zscore)
        
        self.merge_factor(factor)
        # return 1
        
#         factor.columns = [c[0] for c in factor.columns]
#         self.factor_001 = factor
            
    
    def generate_factor_002(self):
        """
        Alpha#2: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
        """
        volume_temp = self.volume
        open_temp = self.open
        close_temp = self.close

        temp_1 = np.log(volume_temp).diff(2)
        temp_1_ranked = temp_1.rank(axis="rows", pct=True)

        temp_2 = (close_temp - open_temp)/open_temp
        temp_2_ranked = temp_2.rank(axis="rows", pct=True)

        factor = -1 * temp_1_ranked.rolling(window=6).corr(temp_2_ranked)

        factor = self.rename_factor_columns(factor, self.coin_data, "002")#.dropna().apply(zscore)
        
        self.merge_factor(factor)
        
#         factor.columns = [c[0] for c in factor.columns]
#         self.factor_002 = factor
    
    def generate_factor_003(self):
        """
        Alpha#3: (-1 * correlation(rank(open), rank(volume), 10))
        """
        temp_open = self.open
        temp_volume = self.volume
        
        ranked_volume = temp_volume.rank(axis="rows")
        ranked_open = temp_open.rank(axis="rows")

        factor = -1 * ranked_volume.rolling(window=10).corr(ranked_open)

        factor = self.rename_factor_columns(factor, self.coin_data, "003")#.dropna().apply(zscore)

        self.merge_factor(factor)
        
#         factor.columns = [c[0] for c in factor.columns]
#         self.factor_003 = factor
        
    def generate_factor_004(self):
        """
        Alpha#4: (-1 * Ts_Rank(rank(low), 9)) 
        """
        temp_low = self.low
        
        rolling_rank = lambda data: data.size - data.argsort().argsort()[-1]

        
        factor = temp_low.rank(axis="rows").rolling(window=9).apply(rolling_rank)
        
        factor = self.rename_factor_columns(factor, self.coin_data, "004")#.dropna().apply(zscore)
        
        self.merge_factor(factor)
        
#         factor.columns = [c[0] for c in factor.columns]
#         self.factor_004 = factor
        
    def generate_factor_nvp(self):
        nvp = self.volume * self.open / self.trade_count
        factor = nvp#.rank(axis="columns", pct=True)#.apply(zscore)
        
        factor = self.rename_factor_columns(factor, self.coin_data, "nvp")
        
        self.merge_factor(factor)
        
        
        