from datetime import datetime, timedelta, time

import ccxt
import pandas as pd
import pytz
from pandera import typing as pt

from Config import config
from Model.MultiTimeframeOHLC import OHLCV
from helper import log


# def zz_seven_days_before_dataframe():
#     sevent_days_before_raw = fetch_last_week_ohlcv()
#     df = pd.DataFrame(sevent_days_before_raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
#     df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
#     df.set_index('date', inplace=True)
#     df.drop(columns=['timestamp'], inplace=True)
#
#     return df


def under_process_date_range(since=None, days: float = 60) -> str:
    end_date = today_morning()
    start_date = end_date - timedelta(days=days)
    return f'{start_date.strftime("%y-%m-%d.%H-%M")}T' \
           f'{end_date.strftime("%y-%m-%d.%H-%M")}'


def last_month_date_range(since=None) -> str:
    start_date = last_month_morning(since)
    end_date = start_date + timedelta(days=30)

    return f'{start_date.strftime("%y-%m-%d.%H-%M")}T' \
           f'{end_date.strftime("%y-%m-%d.%H-%M")}'


def seven_days_before_date_range() -> str:
    _seven_days_before_morning = seven_days_before_morning()
    _end_date = _seven_days_before_morning + timedelta(days=7)

    return f'{_seven_days_before_morning.strftime("%y-%m-%d.%H-%M")}T' \
           f'{_end_date.strftime("%y-%m-%d.%H-%M")}'


def last_month_morning(since=None, tz=pytz.timezone('Asia/Tehran')) -> datetime:
    if since is None:
        since = datetime.now(tz).date()
    start_day = since - timedelta(days=31)

    # to add timezone info back (to get yesterday's morning)
    morning = tz.localize(datetime.combine(start_day, time(0, 0)), is_dst=None)

    return morning


def today_morning(tz=pytz.timezone('Asia/Tehran')) -> datetime:
    return tz.localize(datetime.combine(datetime.now(tz).date(), time(0, 0)), is_dst=None)


def seven_days_before_morning(tz=pytz.timezone('Asia/Tehran')) -> datetime:
    seven_days_before = datetime.now(tz).date() - timedelta(days=7)

    # to add timezone info back (to get yesterday's morning)
    morning = tz.localize(datetime.combine(seven_days_before, time(0, 0)), is_dst=None)

    return morning


# def zz_fetch_last_week_ohlcv(symbol: str = 'BTC/USDT', timeframe=config.timeframes[0]):
#     _seven_days_before_morning = seven_days_before_morning()
#     limit = int(timedelta(days=7) / pd.to_timedelta(timeframe))
#     response = fetch_ohlcv(symbol, timeframe=timeframe, since=_seven_days_before_morning, limit=limit)
#
#     return response


def fetch_ohlcv_by_range(date_range_str: str = None, symbol: str = 'BTC/USDT', timeframe=config.timeframes[0]) \
        -> pt.DataFrame[OHLCV]:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    start_date_string, end_date_string = date_range_str.split('T')
    start_date = datetime.strptime(start_date_string, '%y-%m-%d.%H-%M')
    end_date = datetime.strptime(end_date_string, '%y-%m-%d.%H-%M')
    duration = end_date - start_date
    limit = int(duration / pd.to_timedelta(timeframe))
    response = fetch_ohlcv(symbol, timeframe=timeframe, since=start_date, limit=limit)

    return response


def fetch_ohlcv(symbol, timeframe=config.timeframes[0], since: datetime = None, limit=None, params={}):
    exchange = ccxt.kucoin()

    # Convert pandas timeframe to CCXT timeframe
    ccxt_timeframe = pandas_to_ccxt_timeframes[timeframe]
    output_list = []
    width_of_timeframe = pd.to_timedelta(timeframe).seconds
    max_query_size = 1000
    for batch_start in range(0, limit, max_query_size):
        start_timestamp = int(since.timestamp() + batch_start * width_of_timeframe) * 1000
        this_query_size = min(limit - batch_start, max_query_size)
        log(f'fetch_ohlcv@{datetime.fromtimestamp(start_timestamp / 1000)}#{this_query_size}', stack_trace=False)
        response = exchange.fetch_ohlcv(symbol, timeframe=ccxt_timeframe, since=start_timestamp,
                                        limit=min(limit - batch_start, this_query_size), params=params)
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
