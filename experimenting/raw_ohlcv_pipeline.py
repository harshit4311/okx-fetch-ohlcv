import ccxt
import time
from pprint import pprint

# Connect to OKX
okx = ccxt.okx()
okx.load_markets()
markets = okx.markets

# Tokens you're tracking
token_names = [
    "GP", "BR", "PENDLE", "LAUNCHCOIN", "AURA", "FIST", "MPLX", "BOME",
    "ALCH", "HTXUNION", "AIXBT", "REI", "WBTC", "BAYC", "CETUS"
]

# Manual overrides based on fuzzy matching
manual_symbol_map = {
    "GP": "GPS/USDT:USDT",
    "BR": "BR/USDT:USDT",
    "PENDLE": "PENDLE/USDT",
    "LAUNCHCOIN": "LAUNCH/USDT:USDT",
    "AURA": "AURA/USDT:USDT",
    "FIST": "FIST/USDT:USDT",
    "MPLX": "MPLX/USDT:USDT",
    "BOME": "BOME/USDT",
    "ALCH": "ALCH/USDT:USDT",
    "HTXUNION": "HTX/USDT:USDT",
    "AIXBT": "AIXBT/USDT",
    "REI": "REI/USDT:USDT",
    "WBTC": "WBTC/USDT",
    "BAYC": "BAYC/USDT:USDT",
    "CETUS": "CETUS/USDT"
}

# Fallback: Try to fuzzy-match token if manual not provided
def find_symbol(token):
    for market in markets:
        clean = market.upper().split(":")[0]
        if token.upper() in clean and "/USDT" in market:
            return market
    return None

# Final mapping of token to OKX symbol
token_symbol_map = {}
for token in token_names:
    symbol = manual_symbol_map.get(token) or find_symbol(token)
    if symbol:
        token_symbol_map[token] = symbol
    else:
        print(f"⚠️ Could not find a market for token: {token}")

# Fetch OHLCV for each token
def fetch_token_ohlcv(symbol, timeframe="1d", limit=100):
    try:
        return okx.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None

# Store OHLCV data here
ohlcv_data = {}

for token, symbol in token_symbol_map.items():
    print(f"📈 Fetching OHLCV for {token} ({symbol})")
    data = fetch_token_ohlcv(symbol)
    if data:
        ohlcv_data[token] = data
    time.sleep(1.2)  # Respect API rate limits

# Display sample
print("\n✅ Sample data (first 3 candles):")
for token in ohlcv_data:
    print(f"\n📊 {token} ({token_symbol_map[token]}):")
    for candle in ohlcv_data[token][:3]:
        print(f"  Time: {okx.iso8601(candle[0])}, OHLCV: {candle[1:]}")
