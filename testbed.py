import yfinance as yf
import pandas as pd

stocks = pd.read_csv("RandomStockPicks.page.csv")
symbol_list = stocks['Ticker Symbol'].tolist()
symbol_list.append("AAPL")
symbol_list.append("AMZN")
symbol_list.append("TSLA")

yfws = yf.AsyncWebSocket()
yfws.subscribe(symbol_list)