import ccxt
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta

# === CONFIG ===
DATA_DIR = '/Users/harshit/Downloads/Research-Commons-Quant/OKX-fetch-ohlcv/dataframes'
MIN_TOKEN_AGE_DAYS = 365
MIN_LIQUIDITY = 700_000
MIN_DAYS_WITH_LIQUIDITY = 200
DAYS_BACK = 365
SLEEP = 1.2  # seconds

# === SETUP ===
okx = ccxt.okx()
okx.load_markets()
spot_markets = [m for m in okx.markets if m.endswith('/USDT') and okx.markets[m]['spot']]
end_time = int(datetime(2025, 7, 7).timestamp() * 1000)
start_time = int((datetime(2025, 7, 7) - timedelta(days=DAYS_BACK)).timestamp() * 1000)

# === GET COINGECKO TOKEN LIST ===
print("📥 Fetching CoinGecko tokens...")
coingecko_token_map = {}  # symbol → coingecko_id
cg_list = requests.get("https://api.coingecko.com/api/v3/coins/list").json()
for token in cg_list:
    coingecko_token_map[token['symbol'].lower()] = token['id']

def get_token_genesis_date(symbol):
    fallback_dates = {
        "BTC": "2009-01-03",
        "ETH": "2015-07-30",
        "OKB": "2019-04-30",
        "SOL": "2020-03-20",
        "USDT": "2014-10-06",
        "LTC": "2011-10-13",
        "XRP": "2012-08-02",
        "DOGE": "2013-12-06",
    }

    if symbol.upper() in fallback_dates:
        return datetime.strptime(fallback_dates[symbol.upper()], "%Y-%m-%d")

    token_id = coingecko_token_map.get(symbol.lower())
    if not token_id:
        return None

    url = f"https://api.coingecko.com/api/v3/coins/{token_id}"
    try:
        data = requests.get(url).json()

        # Try primary field
        genesis_date = data.get('genesis_date')
        if genesis_date:
            return datetime.strptime(genesis_date, "%Y-%m-%d")

        # Fallback to ATH date
        ath_date = data.get("market_data", {}).get("ath_date", {}).get("usd")
        if ath_date:
            return datetime.strptime(ath_date[:10], "%Y-%m-%d")

    except Exception as e:
        print(f"❌ Error fetching genesis/ATH for {symbol}: {e}")

    return None


# === MAKE OUTPUT DIR ===
os.makedirs(DATA_DIR, exist_ok=True)
qualified_tokens = []

# === MAIN LOOP ===
for symbol in spot_markets:
    base = okx.markets[symbol]['base']
    print(f"🔍 Checking {symbol}...", end=' ')
    
    genesis = get_token_genesis_date(base)
    if not genesis:
        print("⏭️  No genesis date")
        continue
    age_days = (datetime(2025, 7, 7) - genesis).days
    if age_days < MIN_TOKEN_AGE_DAYS:
        print(f"⏭️  Too young ({age_days} days)")
        continue

    try:
        ohlcv = okx.fetch_ohlcv(symbol, timeframe='1d', since=start_time, limit=DAYS_BACK)
    except Exception as e:
        print(f"❌ Failed OHLCV: {e}")
        continue
    if len(ohlcv) < 30:
        print("⏭️  Too few candles")
        continue

    # Check liquidity
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df['liquidity'] = df['close'] * df['volume']
    days_above = (df['liquidity'] >= MIN_LIQUIDITY).sum()
    if days_above < MIN_DAYS_WITH_LIQUIDITY:
        print(f"⏭️  Low liquidity ({days_above} days)")
        continue

    # Save to CSV
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.to_csv(f"{DATA_DIR}/{base}.csv", index=False)
    qualified_tokens.append({
        'symbol': symbol,
        'token': base,
        'days_above_700k': days_above,
        'total_days': len(df),
        'age_days': age_days
    })
    print(f"✅ Saved ({days_above} days > $700k)")
    time.sleep(SLEEP)

# === SAVE SUMMARY ===
summary_df = pd.DataFrame(qualified_tokens)
summary_df.to_csv(f"{DATA_DIR}/qualified_token_list.csv", index=False)
print(f"\n✅ Done. {len(summary_df)} tokens saved to: {DATA_DIR}")
