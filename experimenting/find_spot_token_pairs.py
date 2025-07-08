import ccxt

# Initialize OKX
okx = ccxt.okx()
okx.load_markets()

# Your token list (base symbols)
tokens = [
    "GP", "BR", "PENDLE", "LAUNCHCOIN", "AURA", "FIST", "MPLX", "BOME", "ALCH",
    "HTXUNION", "AIXBT", "REI", "WBTC", "BAYC", "CETUS", "lvlUSD", "INC", "FBTC"
]

# Create a mapping from base token to actual tradable market
def find_usdt_pair(base_symbol):
    base_symbol = base_symbol.upper()
    for market in okx.markets.values():
        if (
            market['type'] == 'spot' and
            market['quote'] == 'USDT' and
            market['base'] == base_symbol
        ):
            return market['symbol']
    return None

# Run mapping
print("✅ OKX Spot USDT Pairs:\n")
valid = {}
missing = []

for token in tokens:
    symbol = find_usdt_pair(token)
    if symbol:
        print(f"✔️  {token} → {symbol}")
        valid[token] = symbol
    else:
        print(f"❌ {token} → Not found on spot USDT")
        missing.append(token)

# Summary
print("\n📊 Summary:")
print(f"Found: {len(valid)} tokens")
print(f"Missing: {len(missing)} tokens → {missing}")

