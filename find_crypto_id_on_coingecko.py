from pycoingecko import CoinGeckoAPI

coin_gecko_client = CoinGeckoAPI()
looking_symbols = [
    'xrp',
    'btc',
    'ETH',
    'BNB',
    'LTC',
    'BCH',
    'USDC',
    'DOGE',
    'XMR',
    'BUSD',
]

looking_symbols = [x.lower() for x in looking_symbols]

for coin in coin_gecko_client.get_coins_list():
    if coin['symbol'] in looking_symbols:
        print(coin['id'])
