import os

import pandas as pd
import talib as ta
from pandera import typing as pt

from Config import config, GLOBAL_CACHE
from DataPreparation import read_file, single_timeframe
from FigurePlotter.DataPreparation_plotter import plot_ohlca, plot_multi_timeframe_ohlca, plot_multi_timeframe_ohlc, \
    plot_ohlc
from FigurePlotter.plotter import file_id
from MetaTrader import MT
from Model.MultiTimeframeOHLC import MultiTimeframeOHLCV, OHLCV
from Model.MultiTimeframeOHLCA import MultiTimeframeOHLCA, OHLCA
from fetch_ohlcv import fetch_ohlcv_by_range
from helper import measure_time


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


@measure_time
def generate_ohlca(date_range_str: str, file_path: str = config.path_of_data) -> None:
    # if not input_file_path.startswith('ohlc') or input_file_path.startswith('ohlca'):
    #     raise Exception('input_file expected to start with "ohlc" and does not start with "ohlca"!')
    ohlc = read_ohlc(date_range_str)
    ohlca = insert_atr(ohlc)
    # plot_ohlca(ohlca)
    ohlca.to_csv(os.path.join(file_path, f'ohlca.{date_range_str}.zip'), compression='zip')


@measure_time
def generate_multi_timeframe_ohlca(date_range_str: str = None, file_path: str = config.path_of_data) -> None:
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    multi_timeframe_ohlca = pd.DataFrame()
    for _, timeframe in enumerate(config.timeframes):
        _single_timeframe_ohlca = insert_atr(single_timeframe(multi_timeframe_ohlc, timeframe))
        _single_timeframe_ohlca['timeframe'] = timeframe
        _single_timeframe_ohlca.set_index('timeframe', append=True, inplace=True)
        _single_timeframe_ohlca = _single_timeframe_ohlca.swaplevel()
        multi_timeframe_ohlca = pd.concat([_single_timeframe_ohlca, multi_timeframe_ohlca])
    multi_timeframe_ohlc.sort_index(level='date', inplace=True)
    # plot_multi_timeframe_ohlca(multi_timeframe_ohlca)
    multi_timeframe_ohlca.to_csv(os.path.join(file_path, f'multi_timeframe_ohlca.{date_range_str}.zip'),
                                 compression='zip')


@measure_time
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
    multi_timeframe_ohlc.sort_index(inplace=True)
    # plot_multi_timeframe_ohlc(multi_timeframe_ohlc, date_range_str)
    multi_timeframe_ohlc.to_csv(os.path.join(file_path, f'multi_timeframe_ohlc.{date_range_str}.zip'),
                                compression='zip')


# def zz_check_multi_timeframe_ohlca_columns(multi_timeframe_ohlca: pd.DataFrame, raise_exception=False) -> bool:
#     return check_dataframe(multi_timeframe_ohlca, config.multi_timeframe_ohlca_columns, raise_exception)


def read_multi_timeframe_ohlc(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    result = read_file(date_range_str, 'multi_timeframe_ohlc', generate_multi_timeframe_ohlc,
                       MultiTimeframeOHLCV)
    for timeframe in config.timeframes:
        GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
            single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result


def read_multi_timeframe_ohlca(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCA]:
    result = read_file(date_range_str, 'multi_timeframe_ohlca', generate_multi_timeframe_ohlca,
                       MultiTimeframeOHLCA)
    for timeframe in config.timeframes:
        if f'valid_times_{timeframe}' not in GLOBAL_CACHE.keys():
            GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
                single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result


def read_ohlca(date_range_str: str = None) -> pd.DataFrame:
    result = read_file(date_range_str, 'ohlca', generate_ohlca, OHLCA)
    return result


def read_ohlc(date_range_str: str = None) -> pd.DataFrame:
    result = read_file(date_range_str, 'ohlc', generate_ohlc, OHLCV)
    return result


@measure_time
def generate_ohlc(date_range_str: str = None, file_path: str = config.path_of_data):
    # raise Exception('Not implemented so we expect the file exists.')
    # original_prices = pd.read_csv('17-01-01.0-01TO17-12-31.23-59.1min.zip')
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    raw_ohlcv = fetch_ohlcv_by_range(date_range_str)
    df = pd.DataFrame(raw_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('date', inplace=True)
    df.drop(columns=['timestamp'], inplace=True)
    # plot_ohlc(ohlc=df)
    df.to_csv(os.path.join(file_path, f'ohlc.{date_range_str}.zip'),
              compression='zip')
    MT.extract_to_data_path(os.path.join(file_path, f'ohlc.{date_range_str}.zip'),)
