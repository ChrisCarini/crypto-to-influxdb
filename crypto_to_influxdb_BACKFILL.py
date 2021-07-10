from collections import defaultdict
from pprint import pprint

from influxdb import InfluxDBClient
from pycoingecko import CoinGeckoAPI

##
# Variables / Constants
##

DEBUG = False

currencies = [
    {'source': 'binancecoin', 'destination': 'usd'},
    {'source': 'binance-usd', 'destination': 'usd'},
    {'source': 'bitcoin', 'destination': 'usd'},
    {'source': 'bitcoin-cash', 'destination': 'usd'},
    {'source': 'dogecoin', 'destination': 'usd'},
    {'source': 'ethereum', 'destination': 'usd'},
    {'source': 'helium', 'destination': 'usd'},
    {'source': 'litecoin', 'destination': 'usd'},
    {'source': 'monero', 'destination': 'usd'},
    {'source': 'ripple', 'destination': 'usd'},
    {'source': 'usd-coin', 'destination': 'usd'},
]
influxdb_hostname = '192.168.50.20'
influxdb_port = 8086
influxdb_db_name = 'crypto'


##
# Helper Methods
##
def get_historical_data(
        source_currency: str,
        dest_currency: str,
        days: int
):
    price = coin_gecko_client.get_coin_market_chart_by_id(
        id=source_currency,
        vs_currency=dest_currency,
        days=days
    )
    return price


##
# Clients
##
ic = InfluxDBClient(host=influxdb_hostname, port=influxdb_port)
coin_gecko_client = CoinGeckoAPI()

##
# Create & switch to InfluxDB if it does not exist
##
if influxdb_db_name not in [f.get('name') for f in ic.get_list_database()]:
    print(f'InfluxDB DB: [{influxdb_db_name}] does not exist. Creating...')
    ic.create_database(influxdb_db_name)
else:
    print(f'InfluxDB DB: [{influxdb_db_name}] exists...')

# Switch to the Database
print(f'Switching InfluxDB to [{influxdb_db_name}]...')
ic.switch_database(influxdb_db_name)

# We pick the below values, as each gives us a different granularity. From the docs:
#   - Minutely data will be used for duration within 1 day
#   - Hourly data will be used for duration between 1 day and 90 days
#   - Daily data will be used for duration above 90 days.
days_list = [365 * 10, 90, 1]

points = []

for currency in currencies:
    my_data = defaultdict(lambda: dict())

    src_currency = currency['source']
    dst_currency = currency['destination']

    for days in days_list:
        # Fetch Data
        print(f'Fetching {src_currency} to {dst_currency} exchange rate for {days} days...')
        resp = get_historical_data(source_currency=src_currency, dest_currency=dst_currency, days=days)

        for k, v in resp.items():
            for dateee, value in v:
                my_data[dateee][k] = value

    # Insert Data
    for dateee in my_data:
        fields = {}
        if my_data[dateee]['prices'] is not None:
            fields['price'] = float(my_data[dateee]['prices'])
        if my_data[dateee]['market_caps'] is not None:
            fields['market_cap'] = float(my_data[dateee]['market_caps'])
        if my_data[dateee]['total_volumes'] is not None:
            fields['24h_vol'] = float(my_data[dateee]['total_volumes'])

        points.append(
            {
                "measurement": f"{src_currency}_to_{dst_currency}",
                "tags": {
                    "source": "coingecko",
                },
                "time": dateee,
                "fields": fields
            })

    if DEBUG:
        print('Data:')
        pprint(points)

print(f'Writing {len(points)} data points to {influxdb_db_name} database...')
write_result = ic.write_points(points=points, time_precision='ms', batch_size=5000)
if write_result:
    print(f'Success writing {len(points)} data points!')
else:
    print(f'FAILED writing {len(points)} data points!')
