import ccxt
from pycoingecko import CoinGeckoAPI
import pandas as pd
from tqdm import tqdm
import os
import time
from datetime import datetime

cg = CoinGeckoAPI()
okx = ccxt.okx()

DAYS_BACK = 365
LIQUIDITY_THRESHOLD = 700_000
LIQUIDITY_COVERAGE_RATIO = 0.8
VOLUME_THRESHOLD = 1_000_000
MARKET_CAP_MIN = 50_000_000
MARKET_CAP_MAX = 150_000_000
OUTPUT_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/OKX-fetch-ohlcv/strict-filters-df"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load market symbols once
okx.load_markets()
okx_symbols = list(okx.symbols)
usdt_pairs = [s for s in okx_symbols if s.endswith("/USDT")]

# Cache CoinGecko coin list
cg_coins = cg.get_coins_list()

def fetch_ohlcv(symbol):
    now = okx.milliseconds()
    since = now - DAYS_BACK * 24 * 60 * 60 * 1000
    try:
        return okx.fetch_ohlcv(symbol, timeframe='1d', since=since, limit=DAYS_BACK)
    except Exception as e:
        print(f"❌ Error fetching OHLCV for {symbol}: {e}")
        return None

def get_coin_id_from_symbol(symbol):
    matches = [coin for coin in cg_coins if coin['symbol'].lower() == symbol.lower()]
    return matches[0]['id'] if matches else None

def is_in_cap_range(coin_id):
    try:
        data = cg.get_coin_by_id(coin_id)
        mcap = data['market_data']['market_cap']['usd']
        return MARKET_CAP_MIN <= mcap <= MARKET_CAP_MAX
    except Exception:
        return False

def filter_tokens():
    kept = 0
    for symbol in tqdm(usdt_pairs, desc="🔍 Scanning tokens"):
        base = symbol.split("/")[0]

        coin_id = get_coin_id_from_symbol(base)
        if not coin_id:
            continue

        # Filter by market cap
        if not is_in_cap_range(coin_id):
            continue

        ohlcv = fetch_ohlcv(symbol)
        time.sleep(1.3)

        if not ohlcv or len(ohlcv) < DAYS_BACK * 0.8:
            continue

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        df["liquidity_usd"] = df["close"] * df["volume"]
        df["volume_usd"] = df["volume"] * df["close"]

        high_liquidity_days = (df["liquidity_usd"] > LIQUIDITY_THRESHOLD).sum()
        high_volume_days = (df["volume_usd"] > VOLUME_THRESHOLD).sum()

        # Require both filters to be satisfied
        if (
            high_liquidity_days >= LIQUIDITY_COVERAGE_RATIO * len(df) and
            high_volume_days >= LIQUIDITY_COVERAGE_RATIO * len(df)
        ):
            file_path = os.path.join(OUTPUT_DIR, f"{base.replace(':','_')}.csv")
            df.to_csv(file_path, index=False)
            kept += 1
            print(f"✅ Saved {symbol} | {high_liquidity_days} liquidity days, {high_volume_days} volume days")

    print(f"\n🎯 Final tokens saved: {kept}")

if __name__ == "__main__":
    print("📥 Filtering OKX tokens with 1y+ liquidity, 1M+ volume, and mcap between 50M–150M...")
    filter_tokens()
