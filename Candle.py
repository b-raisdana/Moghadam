import os
from datetime import timedelta, datetime

import pandas as pd
import talib as ta
from pandera import typing as pt

from Config import config, GLOBAL_CACHE
from DataPreparation import read_file, single_timeframe, cast_and_validate
from MetaTrader import MT
from Model.MultiTimeframeOHLCV import MultiTimeframeOHLCV, OHLCV
from Model.MultiTimeframeOHLCVA import MultiTimeframeOHLCVA, OHLCVA
from fetch_ohlcv import fetch_ohlcv_by_range
from helper import measure_time, date_range, date_range_to_string


def insert_atr(single_timeframe_ohlcv: pd.DataFrame) -> pd.DataFrame:
    _ATR = ta.ATR(high=single_timeframe_ohlcv['high'].values, low=single_timeframe_ohlcv['low'].values,
                  close=single_timeframe_ohlcv['close'].values, timeperiod=config.ATR_timeperiod)
    single_timeframe_ohlcv['ATR'] = _ATR
    return single_timeframe_ohlcv


@measure_time
def generate_ohlcva(date_range_str: str, file_path: str = config.path_of_data) -> None:
    # if not input_file_path.startswith('ohlcv') or input_file_path.startswith('ohlcva'):
    #     raise Exception('input_file expected to start with "ohlcv" and does not start with "ohlcva"!')
    ohlcv = read_ohlcv(date_range_str)
    ohlcva = insert_atr(ohlcv)
    # plot_ohlcva(ohlcva)
    ohlcva.to_csv(os.path.join(file_path, f'ohlcva.{date_range_str}.zip'), compression='zip')


def expand_date_range(date_range_str: str, time_delta: timedelta, mode: str):
    start, end = date_range(date_range_str)
    if mode == 'start':
        start = start - time_delta
    elif mode == 'end':
        end = end + time_delta
    elif mode == 'both':
        start = start - time_delta
        end = end + time_delta
    else:
        raise Exception(f'mode={mode} not implemented')
    return date_range_to_string(start_date=start, end_date=end)


@measure_time
def generate_multi_timeframe_ohlcva(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    if date_range_str is None:
        date_range_str = config.under_process_date_range

    biggest_timeframe = config.timeframes[-1]
    expanded_date_range = expand_date_range(date_range_str,
                                            time_delta=config.ATR_timeperiod * pd.to_timedelta(biggest_timeframe),
                                            mode='start')
    expanded_multi_timeframe_ohlcv = read_multi_timeframe_ohlcv(expanded_date_range)
    multi_timeframe_ohlcva = pd.DataFrame()
    for _, timeframe in enumerate(config.timeframes):
        _single_timeframe_ohlcva = insert_atr(single_timeframe(expanded_multi_timeframe_ohlcv, timeframe))
        _single_timeframe_ohlcva.dropna(subset=['ATR'], inplace=True)
        _single_timeframe_ohlcva['timeframe'] = timeframe
        _single_timeframe_ohlcva.set_index('timeframe', append=True, inplace=True)
        _single_timeframe_ohlcva = _single_timeframe_ohlcva.swaplevel()
        multi_timeframe_ohlcva = pd.concat([_single_timeframe_ohlcva, multi_timeframe_ohlcva])
    multi_timeframe_ohlcva.sort_index(level='date', inplace=True)
    # plot_multi_timeframe_ohlcva(multi_timeframe_ohlcva)
    multi_timeframe_ohlcva.to_csv(os.path.join(file_path, f'multi_timeframe_ohlcva.{date_range_str}.zip'),
                                  compression='zip')


@measure_time
def generate_multi_timeframe_ohlcv(date_range_str: str, file_path: str = config.path_of_data):
    ohlcv = read_ohlcv(date_range_str)
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
        if pd.to_timedelta(timeframe) >= timedelta(days=1):
            fetch_timeframe_ohlcv = fetch_ohlcv_by_range()
            # Todo: we where here!
        else:
            _timeframe_ohlcv = ohlcv.groupby(pd.Grouper(freq=frequency)) \
                .agg({'open': 'first',
                      'close': 'last',
                      'low': 'min',
                      'high': 'max',
                      'volume': 'sum', })
        if _timeframe_ohlcv.rows > 0:
            _timeframe_ohlcv.insert(0, 'timeframe', timeframe)
            _timeframe_ohlcv.set_index('timeframe', append=True, inplace=True)
            _timeframe_ohlcv = _timeframe_ohlcv.swaplevel()
            multi_timeframe_ohlcv = pd.concat([multi_timeframe_ohlcv, _timeframe_ohlcv])
    multi_timeframe_ohlcv.sort_index(inplace=True, level='date')
    # plot_multi_timeframe_ohlcv(multi_timeframe_ohlcv, date_range_str)
    multi_timeframe_ohlcv.to_csv(os.path.join(file_path, f'multi_timeframe_ohlcv.{date_range_str}.zip'),
                                 compression='zip')


def old_read_multi_timeframe_ohlcv(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    result = read_file(date_range_str, 'multi_timeframe_ohlcv', generate_multi_timeframe_ohlcv,
                       MultiTimeframeOHLCV)
    for timeframe in config.timeframes:
        GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
            single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result


def read_daily_multi_timeframe_ohlcv(day: datetime, timezone='GMT') -> pt.DataFrame[MultiTimeframeOHLCV]:
    # Format the date_range_str for the given day
    start_str = day.strftime('%y-%m-%d.00-00')
    end_str = day.strftime('%y-%m-%d.23-59')
    day_date_range_str = f'{start_str}T{end_str}'

    # Fetch the data for the given day using the old function
    return old_read_multi_timeframe_ohlcv(day_date_range_str)


def read_multi_timeframe_ohlcv(date_range_str: str, precise_start_date=False, precise_end_date=False) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    start, end = date_range(date_range_str)

    # Split the date range into individual days
    current_day = start
    daily_dataframes = []

    while current_day.date() <= end.date():
        # For each day, get the data and append to daily_dataframes list
        daily_dataframes.append(read_daily_multi_timeframe_ohlcv(current_day))
        current_day += timedelta(days=1)

    # Concatenate the daily data and return
    return pd.concat(daily_dataframes)


def read_multi_timeframe_ohlcva(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCVA]:
    result = read_file(date_range_str, 'multi_timeframe_ohlcva', generate_multi_timeframe_ohlcva,
                       MultiTimeframeOHLCVA)
    for timeframe in config.timeframes:
        if f'valid_times_{timeframe}' not in GLOBAL_CACHE.keys():
            GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
                single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result


# def read_daily_multi_timeframe_ohlcva(day: datetime) -> pt.DataFrame[MultiTimeframeOHLCVA]:
#     # Format the date_range_str for the given day
#     start_str = day.strftime('%y-%m-%d.00-00')
#     end_str = day.strftime('%y-%m-%d.23-59')
#     day_date_range_str = f'{start_str}T{end_str}'
#
#     # Fetch the data for the given day using the old function
#     return old_read_multi_timeframe_ohlcva(day_date_range_str)
#
#
# def read_multi_timeframe_ohlcva(date_range_str: str = None) \
#         -> pt.DataFrame[MultiTimeframeOHLCVA]:
#     start, end = date_range(date_range_str)
#
#     # Split the date range into individual days
#     current_day = start
#     daily_dataframes = []
#
#     while current_day.date() <= end.date():
#         # For each day, get the data and append to daily_dataframes list
#         daily_dataframes.append(read_daily_multi_timeframe_ohlcva(current_day))
#         current_day += timedelta(days=1)
#
#     # Concatenate the daily data and return
#     return pd.concat(daily_dataframes)


def read_ohlcva(date_range_str: str = None) -> pt.DataFrame[OHLCVA]:
    result = read_file(date_range_str, 'ohlcva', generate_ohlcva, OHLCVA)
    return result


def old_read_ohlcv(date_range_str: str = None) -> pt.DataFrame[OHLCV]:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    # start_date, end_date = date_range(date_range_str)
    # duration = end_date - start_date
    # if duration < datetime.timedelta(days=1):
    #     raise Exception(f'duration({duration}) less than zero is not acceptable')
    # result = pd.DataFrame()
    # for i in range(0, duration.days):
    #     part_start = start_date + datetime.timedelta(days=i)
    #     part_end = part_start + datetime.timedelta(days=1)
    #     part_date_range = f'{part_start.strftime("%y-%m-%d.%H-%M")}T{part_end.strftime("%y-%m-%d.%H-%M")}'
    #     part_result = read_file(part_date_range, 'ohlcv', generate_ohlcv, OHLCV)
    #     result = pd.concat(part_result)
    result = read_file(date_range_str, 'ohlcv', generate_ohlcv, OHLCV)
    cast_and_validate(result, OHLCV)
    return result

def read_daily_ohlcv(day: datetime, timezone='GMT') -> pt.DataFrame[MultiTimeframeOHLCV]:
    # Format the date_range_str for the given day
    start_str = day.strftime('%y-%m-%d.00-00')
    end_str = day.strftime('%y-%m-%d.23-59')
    day_date_range_str = f'{start_str}T{end_str}'

    # Fetch the data for the given day using the old function
    return old_read_ohlcv(day_date_range_str)


def read_ohlcv(date_range_str: str, precise_start_date=False, precise_end_date=False) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    start, end = date_range(date_range_str)

    # Split the date range into individual days
    current_day = start
    daily_dataframes = []

    while current_day.date() <= end.date():
        # For each day, get the data and append to daily_dataframes list
        daily_dataframes.append(read_daily_ohlcv(current_day))
        current_day += timedelta(days=1)

    # Concatenate the daily data and return
    return pd.concat(daily_dataframes)

@measure_time
def generate_ohlcv(date_range_str: str = None, file_path: str = config.path_of_data):
    # raise Exception('Not implemented, so we expect the file exists.')
    # original_prices = pd.read_csv('17-01-01.0-01TO17-12-31.23-59.1min.zip')
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
