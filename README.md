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

### Output
None; Creates alpha factors in the `./data/coin_data.db` titled `alpha_factors`
## Alpha factor aggregator

## Optimal holdings

