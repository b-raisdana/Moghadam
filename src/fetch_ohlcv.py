from datetime import datetime
from enum import Enum
from typing import List

import ccxt
import pandas as pd
import pytz

from Config import config
from data_preparation import map_symbol
from helper import log, date_range

_ccxt_symbol_map = {
    'BTCUSDT': 'BTC/USDT',
}


class StringCase(Enum):
    Upper = 'upper'
    Lower = 'lower'


def str_list_case(list_of_string: List[str], case: StringCase):
    if case == StringCase.Lower:
        return [x.lower() for x in list_of_string]
    elif case == StringCase.Upper:
        return [x.upper() for x in list_of_string]
    else:
        raise Exception(f'case expected to be a StringCase({[(e.name, e.value) for e in StringCase]}) ')


def map_to_ccxt_symbol(symbol: str) -> str:
    return map_symbol(symbol, _ccxt_symbol_map)


def fetch_ohlcv_by_range(date_range_str: str = None, symbol: str = None, base_timeframe=None,
                         limit_to_under_process_period: bool = None) -> list[object]:
    if limit_to_under_process_period is None:
        limit_to_under_process_period = config.limit_to_under_process_period
    if date_range_str is None:
        date_range_str = config.processing_date_range
    if symbol is None:
        symbol = map_to_ccxt_symbol(config.under_process_symbol)
    if base_timeframe is None:
        base_timeframe = config.timeframes[0]
    start, end = date_range(date_range_str)

    if limit_to_under_process_period:
        _, under_process_period_end = date_range(config.processing_date_range)
        if start > under_process_period_end:
            return []
    duration = end - start + pd.to_timedelta(config.timeframes[0])
    limit = int(duration / pd.to_timedelta(base_timeframe))

    response = fetch_ohlcv(symbol, timeframe=base_timeframe, start=start, limit=limit)
    return response


def fetch_ohlcv(symbol, timeframe: str=None, start: datetime = None, limit=None, params=None) \
        -> list[object]:
    if params is None:
        params = dict()
    assert start.tzinfo == pytz.utc
    exchange = ccxt.kucoin()
    if timeframe is None:
       timeframe = config.timeframes[0]

    # Convert pandas timeframe to CCXT timeframe
    ccxt_timeframe = pandas_to_ccxt_timeframes[timeframe]
    output_list = []
    width_of_timeframe = pd.to_timedelta(timeframe).seconds
    max_query_size = 1000
    for batch_start in range(0, limit, max_query_size):
        if start < datetime.utcnow().replace(tzinfo=pytz.utc):
            start_timestamp = int(start.timestamp() + batch_start * width_of_timeframe) * 1000
            this_query_size = min(limit - batch_start, max_query_size)
            response = exchange.fetch_ohlcv(symbol, timeframe=ccxt_timeframe, since=start_timestamp,
                                            limit=min(limit - batch_start, this_query_size), params=params)
            log(f'fetch_ohlcv@{datetime.fromtimestamp(start_timestamp / 1000)}#{this_query_size}>{len(response)}',
                stack_trace=False)
            output_list = output_list + response

    return output_list


# Dictionary mapping pandas timeframes to CCXT abbreviations
pandas_to_ccxt_timeframes = {
    '1min': '1m',
    '5min': '5m',
    '15min': '15m',
    '30min': '30m',
    '1H': '1h',
    '4H': '4h',
    '1D': '1d',
    '1W': '1w',
    '1M': '1M',  # Note: This is the CCXT abbreviation for 1 month, but it's not precise for trading.
}
