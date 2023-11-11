import os
from datetime import datetime, timedelta

import pandas as pd
import talib as ta
from pandera import typing as pt

from Config import config, GLOBAL_CACHE
from DataPreparation import read_file, trim_to_date_range, single_timeframe, expand_date_range
from Model.MultiTimeframeOHLCV import MultiTimeframeOHLCV
from Model.MultiTimeframeOHLCVA import MultiTimeframeOHLCVA
from helper import measure_time, date_range
from ohlcv import read_base_timeframe_ohlcv, read_multi_timeframe_ohlcv


def insert_atr(single_timeframe_ohlcv: pd.DataFrame) -> pd.DataFrame:
    _ATR = ta.ATR(high=single_timeframe_ohlcv['high'].values, low=single_timeframe_ohlcv['low'].values,
                  close=single_timeframe_ohlcv['close'].values, timeperiod=config.ATR_timeperiod)
    single_timeframe_ohlcv['ATR'] = _ATR
    return single_timeframe_ohlcv


def generate_multi_timeframe_ohlcva(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    start, end = date_range(date_range_str)

    # Split the date range into individual days
    current_day = start
    daily_dataframes = []

    while current_day.date() <= end.date():
        # For each day, get the data and append to daily_dataframes list
        daily_dataframes.append(read_daily_multi_timeframe_ohlcva(current_day))
        current_day += timedelta(days=1)

    # Concatenate the daily data
    # todo: prevent duplicate index (timeframe, date)
    df = pd.concat(daily_dataframes)
    df.sort_index(inplace=True, level='date')
    df = trim_to_date_range(date_range_str, df)
    df.to_csv(os.path.join(file_path, f'multi_timeframe_ohlcva.{date_range_str}.zip'),
              compression='zip')


def read_multi_timeframe_ohlcva(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCVA]:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    result = read_file(date_range_str, 'multi_timeframe_ohlcva', generate_multi_timeframe_ohlcva,
                       MultiTimeframeOHLCV)
    return result


# @measure_time
def core_generate_multi_timeframe_ohlcva(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    if date_range_str is None:
        date_range_str = config.under_process_date_range

    # biggest_timeframe = config.timeframes[-1]
    # expanded_date_range = expand_date_range(date_range_str,
    #                                         time_delta=config.ATR_timeperiod * pd.to_timedelta(biggest_timeframe),
    #                                         mode='start')
    # expanded_multi_timeframe_ohlcv = read_multi_timeframe_ohlcv(expanded_date_range)
    multi_timeframe_ohlcva = pd.DataFrame()
    for _, timeframe in enumerate(config.timeframes):
        expanded_date_range = expand_date_range(date_range_str,
                                                time_delta=(config.ATR_timeperiod + 1) * pd.to_timedelta(timeframe),
                                                mode='start')
        expanded_date_multi_timeframe_ohlcv = read_multi_timeframe_ohlcv(expanded_date_range)
        timeframe_ohlcv = single_timeframe(expanded_date_multi_timeframe_ohlcv, timeframe)
        # _single_timeframe_ohlcva = insert_atr(single_timeframe(expanded_multi_timeframe_ohlcv, timeframe))
        timeframe_ohlcva = insert_atr(timeframe_ohlcv)
        timeframe_ohlcva.dropna(subset=['ATR'], inplace=True)
        timeframe_ohlcva['timeframe'] = timeframe
        timeframe_ohlcva.set_index('timeframe', append=True, inplace=True)
        timeframe_ohlcva = timeframe_ohlcva.swaplevel()
        multi_timeframe_ohlcva = pd.concat([timeframe_ohlcva, multi_timeframe_ohlcva])
    multi_timeframe_ohlcva.sort_index(level='date', inplace=True)

    multi_timeframe_ohlcva = trim_to_date_range(date_range_str, multi_timeframe_ohlcva)
    # plot_multi_timeframe_ohlcva(multi_timeframe_ohlcva)
    multi_timeframe_ohlcva.to_csv(os.path.join(file_path, f'multi_timeframe_ohlcva.{date_range_str}.zip'),
                                  compression='zip')


def core_read_multi_timeframe_ohlcva(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCVA]:
    result = read_file(date_range_str, 'multi_timeframe_ohlcva', core_generate_multi_timeframe_ohlcva,
                       MultiTimeframeOHLCVA)
    for timeframe in config.timeframes:
        if f'valid_times_{timeframe}' not in GLOBAL_CACHE.keys():
            GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
                single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result


def read_daily_multi_timeframe_ohlcva(day: datetime, timezone='GMT') -> pt.DataFrame[MultiTimeframeOHLCV]:
    # Format the date_range_str for the given day
    start_str = day.strftime('%y-%m-%d.00-00')
    end_str = day.strftime('%y-%m-%d.23-59')
    day_date_range_str = f'{start_str}T{end_str}'

    # Fetch the data for the given day using the old function
    return core_read_multi_timeframe_ohlcva(day_date_range_str)

# @measure_time
# def generate_ohlcva(date_range_str: str, file_path: str = config.path_of_data) -> None:
#     ohlcv = read_ohlcv(date_range_str)
#     ohlcva = insert_atr(ohlcv)
#     # plot_ohlcva(ohlcva)
#     ohlcva.to_csv(os.path.join(file_path, f'ohlcva.{date_range_str}.zip'), compression='zip')
# def read_ohlcva(date_range_str: str = None) -> pt.DataFrame[OHLCVA]:
#     result = read_file(date_range_str, 'ohlcva', generate_ohlcva, OHLCVA)
#     return result
