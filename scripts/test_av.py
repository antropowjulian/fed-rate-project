import requests

url = "https://www.alphavantage.co/query"
params = {
    "function"   : "TIME_SERIES_DAILY_ADJUSTED",
    "symbol"     : "SPY",
    "outputsize" : "compact",
    "apikey"     : "94PGYIOFWVU00RDH"
}

r = requests.get(url, params=params)
data = r.json()
print(list(data.keys()))
print(data)

