import os
from datetime import timedelta, datetime

import pandas as pd
from pandera import typing as pt

from Config import config, GLOBAL_CACHE
from DataPreparation import read_file, single_timeframe, cast_and_validate, trim_to_date_range, to_timeframe
from MetaTrader import MT
from Model.MultiTimeframeOHLCV import MultiTimeframeOHLCV, OHLCV
from fetch_ohlcv import fetch_ohlcv_by_range
from helper import measure_time, date_range, date_range_to_string


# @measure_time
def core_generate_multi_timeframe_ohlcv(date_range_str: str, file_path: str = config.path_of_data):
    biggest_timeframe = config.timeframes[-1]
    start, end = date_range(date_range_str)
    round_to_biggest_timeframe_end = to_timeframe(end, biggest_timeframe, ignore_cached_times=True)
    if round_to_biggest_timeframe_end >= start:
        extended_end = (to_timeframe(end, biggest_timeframe, ignore_cached_times=True) + pd.to_timedelta(
            biggest_timeframe) -
                        pd.to_timedelta(config.timeframes[0]))
        extended_timerange_str = date_range_to_string(start=start, end=extended_end)
    else:
        extended_timerange_str = date_range_str

    ohlcv = read_base_timeframe_ohlcv(extended_timerange_str)

    # ohlcv['timeframe '] = config.timeframes[0]
    multi_timeframe_ohlcv = ohlcv.copy()
    multi_timeframe_ohlcv.insert(0, 'timeframe', config.timeframes[0])
    multi_timeframe_ohlcv.set_index('timeframe', append=True, inplace=True)
    multi_timeframe_ohlcv = multi_timeframe_ohlcv.swaplevel()
    for _, timeframe in enumerate(config.timeframes[1:]):
        if timeframe == '1W':
            frequency = 'W-MON'
        elif timeframe == 'M':
            frequency = 'MS'
        else:
            frequency = timeframe
        # if pd.to_timedelta(timeframe) >= timedelta(days=1):
        #     pass
        _timeframe_ohlcv = ohlcv.groupby(pd.Grouper(freq=frequency)) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum', })
        if len(_timeframe_ohlcv.index) > 0:
            _timeframe_ohlcv.insert(0, 'timeframe', timeframe)
            _timeframe_ohlcv.set_index('timeframe', append=True, inplace=True)
            _timeframe_ohlcv = _timeframe_ohlcv.swaplevel()
            multi_timeframe_ohlcv = pd.concat([multi_timeframe_ohlcv, _timeframe_ohlcv])
    multi_timeframe_ohlcv = trim_to_date_range(date_range_str, multi_timeframe_ohlcv)
    multi_timeframe_ohlcv.sort_index(inplace=True, level='date')
    # plot_multi_timeframe_ohlcv(multi_timeframe_ohlcv, date_range_str)
    multi_timeframe_ohlcv.to_csv(os.path.join(file_path, f'multi_timeframe_ohlcv.{date_range_str}.zip'),
                                 compression='zip')


# @measure_time
def core_read_multi_timeframe_ohlcv(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    result = read_file(date_range_str, 'multi_timeframe_ohlcv', core_generate_multi_timeframe_ohlcv,
                       MultiTimeframeOHLCV)
    for timeframe in config.timeframes:
        GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
            single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result


# @measure_time
def read_daily_multi_timeframe_ohlcv(day: datetime) -> pt.DataFrame[MultiTimeframeOHLCV]:
    # Format the date_range_str for the given day
    start_str = day.strftime('%y-%m-%d.00-00')
    end_str = day.strftime('%y-%m-%d.23-59')
    day_date_range_str = f'{start_str}T{end_str}'

    # Fetch the data for the given day using the old function
    return core_read_multi_timeframe_ohlcv(day_date_range_str)


@measure_time
def read_multi_timeframe_ohlcv(date_range_str: str, precise_start_date=False, precise_end_date=False) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    result = read_file(date_range_str, 'multi_timeframe_ohlcv', generate_multi_timeframe_ohlcv,
                       MultiTimeframeOHLCV)
    for timeframe in config.timeframes:
        GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
            single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result


@measure_time
def generate_multi_timeframe_ohlcv(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    start, end = date_range(date_range_str)

    # Split the date range into individual days
    current_day = start
    daily_dataframes = []

    while current_day.date() <= end.date():
        # For each day, get the data and append to daily_dataframes list
        daily_dataframes.append(read_daily_multi_timeframe_ohlcv(current_day))
        current_day += timedelta(days=1)

    # Concatenate the daily data
    # todo: prevent duplicate index (timeframe, date)
    df = pd.concat(daily_dataframes)
    df.sort_index(inplace=True, level='date')
    df = trim_to_date_range(date_range_str, df)
    df.to_csv(os.path.join(file_path, f'multi_timeframe_ohlcv.{date_range_str}.zip'),
              compression='zip')


# @measure_time
def core_read_ohlcv(date_range_str: str = None) -> pt.DataFrame[OHLCV]:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    result = read_file(date_range_str, 'ohlcv', core_generate_ohlcv, OHLCV)
    return result


# @measure_time
def read_daily_ohlcv(day: datetime, timezone='GMT') -> pt.DataFrame[MultiTimeframeOHLCV]:
    # Format the date_range_str for the given day
    start_str = day.strftime('%y-%m-%d.00-00')
    end_str = day.strftime('%y-%m-%d.23-59')
    day_date_range_str = f'{start_str}T{end_str}'

    # Fetch the data for the given day using the old function
    return core_read_ohlcv(day_date_range_str)


@measure_time
def read_base_timeframe_ohlcv(date_range_str: str) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    result = read_file(date_range_str, 'ohlcv', generate_base_timeframe_ohlcv, OHLCV)
    return result


@measure_time
def generate_base_timeframe_ohlcv(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    start, end = date_range(date_range_str)

    # Split the date range into individual days
    current_day = start
    daily_dataframes = []

    while current_day.date() <= end.date():
        # For each day, get the data and append to daily_dataframes list
        daily_dataframes.append(read_daily_ohlcv(current_day))
        current_day += timedelta(days=1)
    # Concatenate the daily data
    # todo: prevent duplicate index (timeframe, date)
    df = pd.concat(daily_dataframes)
    df.sort_index(inplace=True, level='date')
    df = trim_to_date_range(date_range_str, df)
    df.to_csv(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'),
              compression='zip')


# @measure_time
def core_generate_ohlcv(date_range_str: str = None, file_path: str = config.path_of_data):
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    raw_ohlcv = fetch_ohlcv_by_range(date_range_str)
    df = pd.DataFrame(raw_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    # df['date'] = df['date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Tehran')
    df.set_index('date', inplace=True)
    df.drop(columns=['timestamp'], inplace=True)
    # plot_ohlcv(ohlcv=df)
    cast_and_validate(df, OHLCV)
    df.to_csv(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'),
              compression='zip')
    if config.load_data_to_meta_trader:
        MT.extract_to_data_path(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'))
        MT.load_rates()


@measure_time
def load_ohlcv_to_meta_trader(date_range_str: str = None, file_path: str = config.path_of_data):
    if config.load_data_to_meta_trader:
        MT.extract_to_data_path(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'))
        MT.load_rates()
