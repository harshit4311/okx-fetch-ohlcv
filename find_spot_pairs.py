import ccxt

okx = ccxt.okx()
okx.load_markets()
spot_usdt = [s for s in okx.symbols if s.endswith('/USDT') and okx.markets[s]['type'] == 'spot']
print(f"Total spot USDT pairs: {len(spot_usdt)}")
print(spot_usdt[:20])  # sample
