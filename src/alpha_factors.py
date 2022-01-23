
import numpy as np
import pandas as pd
from scipy.stats import zscore
pd.options.display.max_rows = 100
pd.options.display.max_columns = 500

class GenerateAlphas:
    def __init__(self, coin_df):
        
  
        # self.coin_data = coin_df[:-1]
        self.close =        self.extract_and_pivot_fields(coin_df, "close")
        self.mean_bids =    self.extract_and_pivot_fields(coin_df, "mean_bids")
        self.mean_asks =    self.extract_and_pivot_fields(coin_df, "mean_asks")
        self.open_time =    self.extract_and_pivot_fields(coin_df, "open_time")
        self.open =         self.extract_and_pivot_fields(coin_df, "open")
        self.high =         self.extract_and_pivot_fields(coin_df, "high")
        self.low =          self.extract_and_pivot_fields(coin_df, "low")
        self.close =        self.extract_and_pivot_fields(coin_df, "close")
        self.volume =       self.extract_and_pivot_fields(coin_df, "volume")
        self.low =          self.extract_and_pivot_fields(coin_df, "low")
        self.returns =      self.close.pct_change()
    
    extract_and_pivot_fields = lambda self, coin_data, field: coin_data.loc[:,[field, "symbol", "date"]].reset_index().pivot(index="date", columns="symbol").loc[:,field]
    


    def run_factors(self, factors_to_run = "all"):
        af_dict = {
            # "factor_001": self.generate_factor_001,
            # "factor_002": self.generate_factor_002,
            # "factor_003": self.generate_factor_003,
            # "factor_004": self.generate_factor_004, #TODO: I think think this one is not implemented correctly yet
            # "factor_nvp": self.generate_factor_nvp,
            "funding_shift_1": self.generate_funding_shift_1,
            "funding_shift_2": self.generate_funding_shift_2,
            "funding_shift_3": self.generate_funding_shift_3,
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

        factor = factor.reset_index().melt(id_vars = ["date", "factor"])
        try: 
            self.combined_factors = pd.concat([self.combined_factors, factor])
        except AttributeError:
            self.combined_factors = factor

    
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

        # factor = self.rename_factor_columns(factor, self.coin_data, "001")#.dropna().apply(zscore)
        factor["factor"] = "factor_001"
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

        # factor = self.rename_factor_columns(factor, self.coin_data, "002")#.dropna().apply(zscore)
        factor["factor"] = "factor_002"
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

        # factor = self.rename_factor_columns(factor, self.coin_data, "003")#.dropna().apply(zscore)
        factor["factor"] = "factor_003"
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


        
        # factor = self.rename_factor_columns(factor, self.coin_data, "004")#.dropna().apply(zscore)
        factor["factor"] = "factor_004"
        self.merge_factor(factor)
        
#         factor.columns = [c[0] for c in factor.columns]
#         self.factor_004 = factor
        
    def generate_factor_nvp(self):
        nvp = self.volume * self.open / self.trade_count
        factor = nvp#.rank(axis="columns", pct=True)#.apply(zscore)
        
        # factor = self.rename_factor_columns(factor, self.coin_data, "nvp")
        factor["factor"] = "factor_nvp"
        self.merge_factor(factor)
        
        
    def generate_funding_shift_1(self):
        """
        Factor aims to measure the change in demand of a symbol. 
        It compares the current asks / bid ratio (want to purchase over want to sell) with the rolling average over the past 4 hours
        
        """
        shift_1 = ((self.mean_asks/self.mean_bids) - ((self.mean_asks/self.mean_bids).rolling(window = 4).mean()).rank(axis="rows").apply(lambda x: zscore(x, nan_policy='omit')))
        shift_1["factor"] = "funding_shift_1"

        self.merge_factor(shift_1)

        
    def generate_funding_shift_2(self):
        """
        Factor measure the change in demand over the past two hours
        """
        shift_2 = (self.mean_bids/self.mean_asks).rank(axis="rows", pct=True).rolling(window = 2).mean().apply(lambda x: zscore(x, nan_policy='omit'))
        shift_2["factor"] = "funding_shift_2"

        self.merge_factor(shift_2)        

    def generate_funding_shift_3(self):
        
        shift_3 = (self.mean_asks/self.mean_bids).rank(axis="rows", pct=True).rolling(window = 2).mean().apply(lambda x: zscore(x, nan_policy='omit'))
        shift_3["factor"] = "funding_shift_3"

        self.merge_factor(shift_3)