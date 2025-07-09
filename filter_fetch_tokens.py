import ccxt
from pycoingecko import CoinGeckoAPI
import pandas as pd
from tqdm import tqdm
import os
import time
from datetime import datetime, timedelta

cg = CoinGeckoAPI()
okx = ccxt.okx()

DAYS_BACK = 365
LIQUIDITY_THRESHOLD = 700_000
LIQUIDITY_COVERAGE_RATIO = 0.8
OUTPUT_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/OKX-fetch-ohlcv/new-dfs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load market symbols once
okx.load_markets()
okx_symbols = list(okx.symbols)
usdt_pairs = [s for s in okx_symbols if s.endswith("/USDT")]

def fetch_ohlcv(symbol):
    now = okx.milliseconds()
    since = now - DAYS_BACK * 24 * 60 * 60 * 1000
    try:
        return okx.fetch_ohlcv(symbol, timeframe='1d', since=since, limit=DAYS_BACK)
    except Exception as e:
        print(f"❌ Error fetching OHLCV for {symbol}: {e}")
        return None

def get_coin_id_from_symbol(symbol):
    try:
        coins = cg.get_coins_list()
        for coin in coins:
            if coin['symbol'].lower() == symbol.lower():
                return coin['id']
    except:
        return None
    return None

def filter_tokens():
    kept = 0
    for symbol in tqdm(usdt_pairs, desc="🔍 Scanning tokens"):
        base = symbol.split("/")[0]
        ohlcv = fetch_ohlcv(symbol)
        time.sleep(1.3)

        if not ohlcv or len(ohlcv) < DAYS_BACK * 0.8:
            continue

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        # Estimate daily liquidity: volume * price
        df["liquidity_usd"] = df["close"] * df["volume"]
        high_liquidity_days = (df["liquidity_usd"] > LIQUIDITY_THRESHOLD).sum()

        if high_liquidity_days >= LIQUIDITY_COVERAGE_RATIO * len(df):
            # Save if token is liquid enough
            file_path = os.path.join(OUTPUT_DIR, f"{base.replace(':','_')}.csv")
            df.to_csv(file_path, index=False)
            kept += 1
            print(f"✅ Saved {symbol} with {high_liquidity_days} high-liquidity days")
    print(f"\n🎯 Final tokens saved: {kept}")

if __name__ == "__main__":
    print("📥 Fetching OKX tokens with 1y+ liquidity...")
    filter_tokens()
