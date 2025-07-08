import ccxt
from pprint import pprint

okx = ccxt.okx()

symbol = 'GPS/USDT:USDT'   # Replace with your token
timeframe = '1d'        # '1m', '5m', '1h', '1d', '1w', etc.
limit = 100             # Number of candles

ohlcv = okx.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

# Print a few rows
for candle in ohlcv[:5]:
    print(f"Time: {okx.iso8601(candle[0])}, OHLCV: {candle[1:]}")
