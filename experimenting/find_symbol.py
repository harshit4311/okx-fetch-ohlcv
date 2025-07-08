import ccxt

okx = ccxt.okx()
okx.load_markets()

count = 0
for symbol, market in okx.markets.items():
    if market['type'] == 'spot' and market['quote'] == 'USDT':
        print(symbol)
        print(market['info'])  # Dump the raw info from OKX API
        print('-' * 50)
        count += 1
    if count >= 5:
        break
