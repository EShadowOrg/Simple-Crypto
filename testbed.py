import requests

url = "https://eapi.binance.com/api/v3/exchangeInfo"
r = requests.get(url)
print(r.status_code)