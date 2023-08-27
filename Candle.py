import os
import time
from datetime import datetime

import pandas as pd
import pandera
import talib as ta
from pandera import typing as pt

from Config import config, GLOBAL_CACHE
from DataPreparation import check_dataframe, read_file, single_timeframe, timedelta_to_str
from FigurePlotter.DataPreparation_plotter import plot_ohlca, plot_multi_timeframe_ohlca, plot_multi_timeframe_ohlc
from FigurePlotter.plotter import file_id
from helper import log


class OHLC(pandera.DataFrameModel):
    date: pt.Index[datetime]
    open: pt.Series[float]
    close: pt.Series[float]
    high: pt.Series[float]
    low: pt.Series[float]
    volume: pt.Series[float]


class OHLCA(OHLC):
    ATR: pt.Series[float]


def generate_test_ohlc():
    test_ohlc_ticks = pd.read_csv(f'{config.files_to_load[0]}.zip', sep=',', header=0, index_col='date',
                                  parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
    file_name = f'ohlc.{file_id(test_ohlc_ticks)}.zip'
    test_ohlc_ticks.to_csv(file_name, compression='zip')


def insert_atr(single_timeframe_ohlc: pd.DataFrame) -> pd.DataFrame:
    _ATR = ta.ATR(high=single_timeframe_ohlc['high'].values, low=single_timeframe_ohlc['low'].values,
                  close=single_timeframe_ohlc['close'].values)
    single_timeframe_ohlc['ATR'] = _ATR
    return single_timeframe_ohlc


def generate_ohlca(date_range_str: str, file_path: str = config.path_of_data) -> None:
    # if not input_file_path.startswith('ohlc') or input_file_path.startswith('ohlca'):
    #     raise Exception('input_file expected to start with "ohlc" and does not start with "ohlca"!')
    ohlc = read_ohlc(date_range_str)
    ohlca = insert_atr(ohlc)
    plot_ohlca(ohlca)
    ohlca.to_csv(os.path.join(file_path, f'ohlca.{date_range_str}.zip'), compression='zip')


def generate_multi_timeframe_ohlca(date_range_str: str = config.under_process_date_range,
                                   file_path: str = config.path_of_data) -> None:
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    multi_timeframe_ohlca = pd.DataFrame()
    for _, timeframe in enumerate(config.timeframes):
        _single_timeframe_ohlca = insert_atr(single_timeframe(multi_timeframe_ohlc, timeframe))
        _single_timeframe_ohlca['timeframe'] = timeframe
        _single_timeframe_ohlca.set_index('timeframe', append=True, inplace=True)
        _single_timeframe_ohlca = _single_timeframe_ohlca.swaplevel()
        multi_timeframe_ohlca = pd.concat([_single_timeframe_ohlca, multi_timeframe_ohlca])
    multi_timeframe_ohlc.sort_index(level='date', inplace=True)
    plot_multi_timeframe_ohlca(multi_timeframe_ohlca)
    multi_timeframe_ohlca.to_csv(os.path.join(file_path, f'multi_timeframe_ohlca.{date_range_str}.zip'),
                                 compression='zip')


def generate_multi_timeframe_ohlc(date_range_str: str, file_path: str = config.path_of_data):
    ohlc = read_ohlc(date_range_str)
    # ohlc['timeframe '] = config.timeframes[0]
    multi_timeframe_ohlc = ohlc.copy()
    multi_timeframe_ohlc.insert(0, 'timeframe', config.timeframes[0])
    multi_timeframe_ohlc.set_index('timeframe', append=True, inplace=True)
    multi_timeframe_ohlc = multi_timeframe_ohlc.swaplevel()
    for _, timeframe in enumerate(config.timeframes[1:]):
        _timeframe_ohlc = ohlc.groupby(pd.Grouper(freq=timeframe)) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum', })
        _timeframe_ohlc.insert(0, 'timeframe', timeframe)
        _timeframe_ohlc.set_index('timeframe', append=True, inplace=True)
        _timeframe_ohlc = _timeframe_ohlc.swaplevel()
        multi_timeframe_ohlc = pd.concat([multi_timeframe_ohlc, _timeframe_ohlc])
    multi_timeframe_ohlc = multi_timeframe_ohlc.sort_index()
    plot_multi_timeframe_ohlc(multi_timeframe_ohlc, date_range_str)
    multi_timeframe_ohlc.to_csv(os.path.join(file_path, f'multi_timeframe_ohlc.{date_range_str}.zip'),
                                compression='zip')


def check_multi_timeframe_ohlca_columns(multi_timeframe_ohlca: pd.DataFrame, raise_exception=False) -> bool:
    return check_dataframe(multi_timeframe_ohlca, config.multi_timeframe_ohlca_columns, raise_exception)


def read_multi_timeframe_ohlc(date_range_str: str = config.under_process_date_range) -> pd.DataFrame:
    mult_timeframe_ohlc = read_file(date_range_str, 'multi_timeframe_ohlc', generate_multi_timeframe_ohlc)
    for timeframe in config.timeframes:
        GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
            single_timeframe(mult_timeframe_ohlc, timeframe).index.get_level_values('date').tolist()
    return mult_timeframe_ohlc


def read_multi_timeframe_ohlca(date_range_str: str = config.under_process_date_range) -> pd.DataFrame:
    mult_timeframe_ohlca = read_file(date_range_str, 'multi_timeframe_ohlca', generate_multi_timeframe_ohlca)
    for timeframe in config.timeframes:
        if f'valid_times_{timeframe}' not in GLOBAL_CACHE.keys():
            GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
                single_timeframe(mult_timeframe_ohlca, timeframe).index.get_level_values('date').tolist()
    return mult_timeframe_ohlca


def read_ohlca(date_range_string: str) -> pd.DataFrame:
    return read_file(date_range_string, 'ohlca', generate_ohlca)


def read_ohlc(date_range_string: str) -> pd.DataFrame:
    # try:
    return read_file(date_range_string, 'ohlc', generate_ohlc)
    # except:
    #     log(f'Failed to load ohlc.{date_range_string} try to load ohlca.{date_range_string}')
    #     return read_file(date_range_string, 'ohlca', generate_ohlc)


def generate_ohlc(date_range_string: str):
    raise Exception('Not implemented so we expect the file exists.')
    original_prices = pd.read_csv('17-01-01.0-01TO17-12-31.23-59.1min.zip')
