import ccxt
import time
import pandas as pd
from datetime import datetime, timedelta
import os

# Connect to OKX
okx = ccxt.okx()
okx.load_markets()

# Tokens that worked
token_symbol_map = {
    "GP": "GPS/USDT:USDT",
    "PENDLE": "PENDLE/USDT",
    "BOME": "BOME/USDT",
    "ALCH": "ALCH/USDT:USDT",
    "AIXBT": "AIXBT/USDT",
    "WBTC": "WBTC/USDT",
    "CETUS": "CETUS/USDT"
}

# Output directory
output_dir = "/Users/harshit/Downloads/Research-Commons-Quant/OKX-fetch-ohlcv/dataframes"
os.makedirs(output_dir, exist_ok=True)

# Date config
end_date = datetime(2025, 7, 7)
start_date = end_date - timedelta(days=100)
since_timestamp = int(okx.parse8601(start_date.isoformat() + "Z"))

def fetch_ohlcv(symbol, since, timeframe="1d", limit=100):
    try:
        ohlcv = okx.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        return ohlcv
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None

# Fetch & save to CSV
for token, symbol in token_symbol_map.items():
    print(f"📈 Fetching OHLCV for {token} ({symbol})...")
    data = fetch_ohlcv(symbol, since=since_timestamp, timeframe="1d", limit=110)
    if not data:
        continue

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df[["datetime", "open", "high", "low", "close", "volume"]]
    df = df.sort_values("datetime")

    # Filter to last 100 days (in case extra rows)
    df = df[df["datetime"] <= end_date].tail(100)

    # Save
    output_path = os.path.join(output_dir, f"{token}.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved {output_path} with {len(df)} rows.")

    time.sleep(1.2)  # Respect rate limits
