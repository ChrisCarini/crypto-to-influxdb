import itertools
import logging
import os
import pprint
import signal
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from influxdb import InfluxDBClient
from pycoingecko import CoinGeckoAPI

##
# Setup Logging
##
logger = logging.getLogger()
formatter = logging.Formatter(fmt="%(asctime)s - [%(levelname)s] - %(message)s")

stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = RotatingFileHandler("crypto_to_influxdb.log", backupCount=5, maxBytes=1000000)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

##
# Load Configuration
##
logger.info("Loading configuration from [config.yaml]...")
with open('config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)


def get_config_value(key: str, default: Any) -> Any:
    """Get the configuration value for the provided key. If none found, return the default.

    :param key: The configuration key
    :param default: The default value, should no value be set in the configuration
    :return: The configuration value, or 'default' value should one not be set.
    """
    return config.get(key, os.environ.get(key, default))


##
# Environment Variables
##

DEBUG = True if str(get_config_value('DEBUG_FLAG', 'false')).lower() == 'true' else False  # False
logger.info(f'DEBUG_FLAG: {DEBUG}\n')
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

INFLUXDB_HOST = str(get_config_value('INFLUXDB_HOST', ''))  # '192.168.50.20'
INFLUXDB_PORT = int(get_config_value('INFLUXDB_PORT', 0))  # 8086
INFLUXDB_DB_NAME = str(get_config_value('INFLUXDB_DB_NAME', ''))  # 'crypto'

# Env Var input validation (basic.)
if INFLUXDB_HOST == '' or INFLUXDB_PORT == 0 or INFLUXDB_DB_NAME == '':
    logger.info(f'One of the input environment variables is not set correctly.')
    logger.info(f'    DEBUG_FLAG:         {DEBUG}')
    logger.info(f'    INFLUXDB_HOST:      {INFLUXDB_HOST}')
    logger.info(f'    INFLUXDB_PORT:      {INFLUXDB_PORT}')
    logger.info(f'    INFLUXDB_DB_NAME:   {INFLUXDB_DB_NAME}')
    logger.info(f'')
    logger.info(f'Please check the environment variables and start the container again. Exiting.')
    exit(1)

##
# Variables / Constants
##
interval_time_sec = 60

logger.debug(f'Running with the below variables:')
logger.debug(f'  Environment Variables')
logger.debug(f'    DEBUG_FLAG:               {DEBUG}')
logger.debug(f'    INFLUXDB_HOST:            {INFLUXDB_HOST}')
logger.debug(f'    INFLUXDB_PORT:            {INFLUXDB_PORT}')
logger.debug(f'    INFLUXDB_DB_NAME:         {INFLUXDB_DB_NAME}')
logger.debug(f'  Constants')
logger.debug(f'    interval_time_sec:        {interval_time_sec}')
for currency in config['currencies']:
    logger.debug(f'    currencies[sources]:      {",".join(set([x for x in currency["source"]]))}')
    logger.debug(f'    currencies[destinations]: {",".join(set([x for x in currency["destination"]]))}')
    logger.debug(f'{"=" * 40}')


##
# Helper Methods
##
def get_data(source_currency: str, dest_currency: str):
    price = coin_gecko_client.get_price(
        ids=source_currency,
        vs_currencies=dest_currency,
        include_market_cap='true',
        include_24hr_vol='true',
        include_24hr_change='true',
        include_last_updated_at='true'
    )
    return price


##
# Clients
##
scheduler = BlockingScheduler()
ic = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT)
coin_gecko_client = CoinGeckoAPI()

##
# Create & switch to InfluxDB if it does not exist
##
if INFLUXDB_DB_NAME not in [f.get('name') for f in ic.get_list_database()]:
    logger.info(f'InfluxDB DB: [{INFLUXDB_DB_NAME}] does not exist. Creating...')
    ic.create_database(INFLUXDB_DB_NAME)
else:
    logger.info(f'InfluxDB DB: [{INFLUXDB_DB_NAME}] exists...')

# Switch to the Database
logger.info(f'Switching InfluxDB to [{INFLUXDB_DB_NAME}]...')
ic.switch_database(INFLUXDB_DB_NAME)


def job():
    logger.info('==========================================')
    logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Starting next run...')
    points = []
    for currency in config['currencies']:
        for src_currency, dst_currency in itertools.product(currency['source'], currency['destination']):
            # Fetch Data
            logger.debug(f'Fetching {src_currency} to {dst_currency} exchange rate...')

            resp = get_data(source_currency=src_currency, dest_currency=dst_currency)
            price = resp.get(src_currency, {}).get(dst_currency)
            data_time = resp.get(src_currency, {}).get('last_updated_at')
            logger.info(f'RECEIVED: {src_currency} in {dst_currency} is ${price} - at {data_time}')

            # Append Data
            points.append(
                {
                    "measurement": f"{src_currency}_to_{dst_currency}",
                    "tags": {
                        "source": "coingecko",
                    },
                    "time": data_time,
                    "fields": {
                        "price": float(resp.get(src_currency, {}).get(dst_currency)),
                        "market_cap": float(resp.get(src_currency, {}).get(f'{dst_currency}_market_cap')),
                        "24h_vol": float(resp.get(src_currency, {}).get(f'{dst_currency}_24h_vol')),
                        "24h_change": float(resp.get(src_currency, {}).get(f'{dst_currency}_24h_change')),
                    }
                }
            )

    # Insert Data
    logger.debug('Data:')
    logger.debug(pprint.pformat(points, indent=4))

    logger.info(f'Writing to {INFLUXDB_DB_NAME} database...')
    write_result = ic.write_points(points=points, time_precision='s', batch_size=5000)
    if write_result:
        logger.info(f'Success writing {len(points)} data point(s)!')
    else:
        logger.error(f'FAILED writing {len(points)} data point(s)!')


logger.info(f'Adding job to run every {interval_time_sec} seconds...')
scheduler.add_job(
    func=job,
    trigger='interval',
    seconds=interval_time_sec,
    next_run_time=datetime.now()
)
logger.info(f'Starting job [{scheduler}] ...')
scheduler.start()


def gracefully_exit(signum, frame):
    logger.info('Stopping scheduler...')
    scheduler.shutdown()


signal.signal(signal.SIGINT, gracefully_exit)
signal.signal(signal.SIGTERM, gracefully_exit)
