import json
import requests
import argparse
import pandas as pd
import datetime as dt
import time

from typing import Optional
from utils import string_to_timestamp
from paths import SPOT_DATA_PATH
from variables import MINUTE_1, HALF_DAY, FULL_DAY

BASE_SPOT_DATA_URL = "https://api.binance.com/api/v3/klines"


def download_tracker(func):
    def wrapper(*args):
        start = dt.datetime.now()
        print(f"| Download START | {args[0]}({args[1]}), {args[2]} ~ {args[3]}")
        func(*args)
        end = dt.datetime.now()
        time_taken = (end - start).total_seconds()
        print(f"| Download FINISH | {args[0]}({args[1]}), {args[2]} ~ {args[3]}")
        print(f"time taken: {time_taken//60} mins {round(time_taken%60,2)} seconds")
        return func(*args)

    return wrapper


def kline_data_from_api(params: dict, url: str = BASE_SPOT_DATA_URL) -> pd.DataFrame:
    """_summary_

    Args:
        params (dict): _description_
        url (_type_, optional): _description_. Defaults to BASE_SPOT_DATA_URL.

    Returns:
        pd.DataFrame: _description_
    """
    # get kline data given params
    data = pd.DataFrame(json.loads(requests.get(url, params=params).text))
    data.columns = [
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "qav",
        "num_trades",
        "taker_base_vol",
        "taker_quote_vol",
        "ignore",
    ]
    data.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in data.datetime]
    extract_item = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "num_trades",
        "taker_base_vol",
    ]
    return data.loc[:, extract_item]


@download_tracker
def fetch_kline_data(
    symbol: str,
    interval: str,
    start_date: str,
    end_date: str,
    url: Optional[str] = None,
) -> pd.DataFrame:
    """_summary_

    Args:
        symbol (str): _description_
        interval (str): _description_
        start_date (str): _description_
        end_date (str): _description_
        url (Optional[str], optional): _description_. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """

    if url is None:
        url = BASE_SPOT_DATA_URL

    start_date = string_to_timestamp(start_date)
    end_date = string_to_timestamp(end_date) + FULL_DAY
    cut_point = start_date + HALF_DAY

    agg_data = []
    while cut_point <= end_date:
        # fetch data based on the params dictionary.
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": str(start_date * 1000),
            "endTime": str((cut_point - MINUTE_1) * 1000),
            "limit": 1000,
        }

        # append to the list.
        agg_data.append(kline_data_from_api(params, url))

        # shift the viewing window
        start_date = cut_point
        cut_point = start_date + HALF_DAY
        time.sleep(1)

    agg_data = pd.concat(agg_data, axis=0)
    agg_data = agg_data.drop_duplicates()
    return agg_data.astype(float)


if __name__ == "__main__":
    # Initialize argument parser
    parser = argparse.ArgumentParser(description="Binance SPOT Data Downloader")

    # Set arguments to receive (default is set)
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--interval", type=str, default="1m")
    parser.add_argument("--start_date", type=str, default="2015-01-01")
    parser.add_argument(
        "--end_date", type=str, default=dt.datetime.today().strftime("%Y-%m-%d")
    )

    # Parse arguments
    args = parser.parse_args()

    # fetch data from Binance API
    data = fetch_kline_data(args.symbol, args.interval, args.start_date, args.end_date)
    data.to_csv(SPOT_DATA_PATH + f"{args.symbol.lower()}_spot.csv")
