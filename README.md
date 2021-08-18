# Overview of process
## Downloader
Main function: `run_downloader.py`

### Overview 
Function that downloads the latest data from Binance and saves it into a sqlite3 database under `./data/crypto.db`. Function checks if enough time has passed since last download (ie more than hour has passed) and if Binance has the latest data available by downloading a sample. 


### Inputs
`./config/tickers.txt` List if tickers to download. List gathered from 
    
`./config/tickers_to_ignore.txt` Somehwat redudant, might patch this out eventuall. 

`./config/binance_secret_key.txt` Personal secret key. Listed in .gitignore

`./config/binance_api_key.txt` Binance API key. Also listed in .gitignore

### Output
None; Creates database ./data/coin_data.db and stores all of the data in a table titled `coin_data`

TODO: Currently I use multiprocessing to split out the downloading tasks - but one can do this more effectively with async 

## Alphafactror generator

### Overview 
Runs the functions that are defined in `./src/alpha_factors.py`. The main function checks if there's new data available since the last run. If there is, it will create all of the nominated alpha factors. 

### Inputs
Coin data from `./data/coin_data.db`

Functions from `./src/alpha_factors.py`

### Output
None; Creates alpha factors in the `./data/coin_data.db` titled `alpha_factors`

## Alpha factor aggregator
### Overview


The intention if this function is to aggregate the alpha factor outputs. Currently it uses as ranking method to create weights for each alpha factor for each coin. It goes about doing that by binning the returns into 5 bins based on quantiles (0.025, 0.15, 0.85, 0.975) with bin valuse -2, -1, 0, 1, and 2. The alpha factor values are placed in two bins around the 0.5 quantile with values -1 and 1. The process takes the product of the returns bin and the alpha bin values. Summing over a given time period will give you weights which are indicative on each alpha factor's performance. These weights are then converted into values which would sum up to one. 



## Optimal holdings

