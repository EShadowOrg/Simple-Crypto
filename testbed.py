import yfinance as yf
import pandas as pd

stocks = pd.read_csv("RandomStockPicks.page.csv")
symbol_list = stocks['Ticker Symbol'].tolist()
symbol_list.append("AAPL")

yfws = yf.WebSocket()
yfws.subscribe(symbol_list)
yfws.listen()