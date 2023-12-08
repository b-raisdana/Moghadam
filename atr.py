import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_ta as ta  # noqa
import pytz
import talib as tal
from pandas import Timestamp
from pandera import typing as pt

from Config import config, GLOBAL_CACHE
from data_preparation import read_file, trim_to_date_range, single_timeframe, expand_date_range, \
    multi_timeframe_times_tester, empty_df
from Model.MultiTimeframeOHLCV import MultiTimeframeOHLCV, OHLCV
from Model.MultiTimeframeOHLCVA import MultiTimeframeOHLCVA
from helper import date_range, measure_time, log
from ohlcv import read_multi_timeframe_ohlcv


def RMA(values: pd.DataFrame, length):
    # alpha = 1 / length
    # rma = np.zeros_like(values)
    # rma[0] = values[0]
    # for i in range(1, len(values)):
    #     rma[i] = alpha * values[i] + np.nan_to_num((1 - alpha) * rma[i - 1])
    #     pass
    # return rma
    alpha = 1 / length
    rma = pd.DataFrame(values.index, np.nan)  # Initialize with NaN

    # Find the first non-NaN value in the series
    first_valid_index = values.first_valid_index()
    if first_valid_index is None:
        return rma  # Return as all NaN if no valid values

    rma[first_valid_index] = values[first_valid_index]  # Start with the first valid value

    for i in range(first_valid_index + 1, len(values)):
        rma[i] = alpha * values[i] + (1 - alpha) * rma[i - 1]

    return rma


@measure_time
def insert_atr(timeframe_ohlcv: pt.DataFrame[OHLCV], mode: str = 'pandas_ta', apply_rma: bool = True) -> pd.DataFrame:
    if len(timeframe_ohlcv) <= config.ATR_timeperiod:
        timeframe_ohlcv['ATR'] = pd.NA
    else:
        if mode == 'pandas_ta':
            timeframe_ohlcv['ATR'] = timeframe_ohlcv.ta.atr(timeperiod=config.ATR_timeperiod,
                                                            # high='high',
                                                            # low='low',
                                                            # close='close',
                                                            # mamode='ema',
                                                            )
        elif mode == 'ta_lib':
            timeframe_ohlcv['ATR'] = tal.ATR(high=timeframe_ohlcv['high'].values, low=timeframe_ohlcv['low'].values,
                                             close=timeframe_ohlcv['close'].values, timeperiod=config.ATR_timeperiod)
        else:
            raise Exception(f"Unsupported mode:{mode}")
    return timeframe_ohlcv


def generate_multi_timeframe_ohlcva(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    if date_range_str is None:
        date_range_str = config.processing_date_range

    start, end = date_range(date_range_str)

    # Split the date range into individual days
    current_day = start
    daily_dataframes = []

    while current_day.date() <= end.date():
        # For each day, get the data and append to daily_dataframes list
        daily_dataframes.append(read_daily_multi_timeframe_ohlcva(current_day))
        current_day += timedelta(days=1)

    # Concatenate the daily data
    df = pd.concat(daily_dataframes)
    df.sort_index(inplace=True, level='date')
    df = trim_to_date_range(date_range_str, df)
    # assert not df.index.duplicated().any()
    assert multi_timeframe_times_tester(df, date_range_str)
    df.to_csv(os.path.join(file_path, f'multi_timeframe_ohlcva.{date_range_str}.zip'),
              compression='zip')


def read_multi_timeframe_ohlcva(date_range_str: str = None) \
        -> MultiTimeframeOHLCVA:
    if date_range_str is None:
        date_range_str = config.processing_date_range
    result = read_file(date_range_str, 'multi_timeframe_ohlcva', generate_multi_timeframe_ohlcva,
                       MultiTimeframeOHLCVA)
    return result


@measure_time
def core_generate_multi_timeframe_ohlcva(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    if date_range_str is None:
        date_range_str = config.processing_date_range

    # biggest_timeframe = config.timeframes[-1]
    # expanded_date_range = expand_date_range(date_range_str,
    #                                         time_delta=config.ATR_timeperiod * pd.to_timedelta(biggest_timeframe),
    #                                         mode='start')
    # expanded_multi_timeframe_ohlcv = read_multi_timeframe_ohlcv(expanded_date_range)
    multi_timeframe_ohlcva = empty_df(MultiTimeframeOHLCVA)
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
    assert multi_timeframe_times_tester(multi_timeframe_ohlcva, date_range_str)
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


def read_daily_multi_timeframe_ohlcva(day: datetime, timezone='GMT') -> pt.DataFrame[MultiTimeframeOHLCVA]:
    # Format the date_range_str for the given day
    start_str = day.strftime('%y-%m-%d.00-00')
    end_str = day.strftime('%y-%m-%d.23-59')
    day_date_range_str = f'{start_str}T{end_str}'

    if day.replace(hour=0, minute=0, second=0, microsecond=0) > datetime.now(tz=pytz.UTC):
        return empty_df(MultiTimeframeOHLCVA)
    # Fetch the data for the given day using the old function
    return core_read_multi_timeframe_ohlcva(day_date_range_str)
