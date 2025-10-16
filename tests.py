from src.simple_crypto.market_access import MarketAccess
import time
from src.simple_crypto import misc


def test_market_access():
    log_file = misc.get_log_file()
    logger = MarketAccess.BaseLogger(filenames=[log_file])
    market = MarketAccess(logger=logger)
    market.start()
    time.sleep(5)
    btctracker = MarketAccess.BaseTracker("BTC", market)
    ethtracker = MarketAccess.BaseTracker("ETH", market)
    market.subscribe("BTC", btctracker, event="trade")
    market.subscribe("ETH", ethtracker, event="trade")
    time.sleep(10)
    market.stop()

def test_historical_data():
    MarketAccess.get_history("BTC", "USD", "1m", 1)
    MarketAccess.get_history("ETH", "USD", "1h", 1)

if __name__ == "__main__":
    test_historical_data()