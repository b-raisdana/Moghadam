import typing

import pandas as pd
import talib as ta

from Config import config
from helper import log


def generate_test_ohlc():
    test_ohlc_ticks = pd.read_csv(f'{config.files_to_load[0]}.zip', sep=',', header=0, index_col='date',
                                  parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
    file_name = f'ohlc.{test_ohlc_ticks.index[0].strftime("%y-%m-%d.%H-%M")}T' \
                f'{test_ohlc_ticks.index[-1].strftime("%y-%m-%d.%H-%M")}.zip'
    test_ohlc_ticks.to_csv(file_name, compression='zip')


def insert_atr(ohlc: pd.DataFrame) -> pd.DataFrame:
    _ATR = ta.ATR(high=ohlc['high'].values, low=ohlc['low'].values, open=ohlc['open'].values,
                  close=ohlc['close'].values)
    ohlc['ATR'] = _ATR
    return ohlc


def convert_to_ohlca_csv(input_file_path: str) -> None:
    if not input_file_path.startswith('ohlc') or input_file_path.startswith('ohlca'):
        raise Exception('input_file expected to start with "ohlc" and does not start with "ohlca"!')
    ohlc = pd.read_csv(input_file_path, sep=',', header=0, index_col='date', parse_dates=['date'])
    ohlca = insert_atr(ohlc)
    ohlca.to_csv(input_file_path.replace('ohlc', 'ohlca', 1), compression='zip')


def generate_multi_time_ohlca(date_range_str: str = config.under_process_date_range) -> None:
    multi_time_ohlca = read_multi_time_ohlc(date_range_str)
    for i, time in enumerate(config.timeframes[1:]):
        multi_time_ohlca.at[multi_time_ohlca['timeframe'] == time, 'ATR'] = ta.ATR(
            open=multi_time_ohlca.loc[multi_time_ohlca['timeframe'] == time, 'open'],
            high=multi_time_ohlca.loc[multi_time_ohlca['timeframe'] == time, 'high'],
            low=multi_time_ohlca.loc[multi_time_ohlca['timeframe'] == time, 'low'],
            close=multi_time_ohlca.loc[multi_time_ohlca['timeframe'] == time, 'close'],
        )
    return multi_time_ohlca


def read_multi_time_ohlca(date_range_str: str = config.under_process_date_range) -> pd.DataFrame:
    # multi_time_ohlca = None
    # try:
    #     multi_time_ohlca = pd.read_csv(f'multi_time_ohlc.{date_range_str}.zip', sep=',', header=0, index_col='date',
    #                                    parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
    # if not check_multi_time_ohlca_columns(multi_time_ohlca):
    #     generate_multi_time_ohlca(date_range_str)
    #     multi_time_ohlca = pd.read_csv(f'multi_time_ohlc.{date_range_str}.zip', sep=',', header=0, index_col='date',
    #                                    parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
    #     if not check_multi_time_ohlca_columns(multi_time_ohlca):
    #         raise Exception(f'Failed to generate_multi_time_ohlca.
    #               multi_time_ohlca.columns:{multi_time_ohlca.columns}')
    return read_file(date_range_str, 'multi_time_ohlca', generate_multi_time_ohlca)


def generate_multi_time_ohlc(date_range_str: str):
    ohlc = read_ohlc(date_range_str)
    for _, time in enumerate(config.timeframes[1:]):
        _time_ohlc_ticks = ohlc.groupby(pd.Grouper(freq=config.timeframes[_])) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum'})
        _time_ohlc_ticks['timeframe'] = time
        ohlc = pd.concat([ohlc, _time_ohlc_ticks]).sort_index()
    ohlc.to_csv(f'multi_time_ohlc.{date_range_str}.zip', compression='zip')


def read_multi_time_ohlc(date_range_str: str = config.under_process_date_range) -> pd.DataFrame:
    return read_file(date_range_str, 'multi_time_ohlca', generate_multi_time_ohlc)


def read_file(date_range_str: str, data_frame_type: str, generator: typing.Callable) -> pd.DataFrame:
    df = None
    try:
        df = pd.read_csv(f'{data_frame_type}.{date_range_str}.zip', sep=',', header=0, index_col='date',
                         parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
    except:
        pass
    if (data_frame_type + '_columns') not in dict(config).__dir__():
        raise Exception(data_frame_type + '_columns not defined in configuration!')
    if not check_dataframe(df, dict(config)[data_frame_type + '_columns']):
        # generator_name = 'generate_' + data_frame_type
        # generator_func = generator_name()
        # if not callable(generator_func):
        #     raise Exception(f'{generator_func}() is not callable!')
        generator(date_range_str)
        df = pd.read_csv(f'{data_frame_type}.{date_range_str}.zip', sep=',', header=0, index_col='date',
                         parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
        if not check_dataframe(df, dict(config)[data_frame_type + '_columns']):
            raise Exception(f'Failed to generate {data_frame_type}! {data_frame_type}.columns:{df.columns}')
    return df


def check_multi_time_ohlca_columns(multi_time_ohlca: pd.DataFrame, raise_exception=False) -> bool:
    return check_dataframe(multi_time_ohlca, config.multi_time_ohlca_columns, raise_exception)


def check_dataframe(data_frame: pd.DataFrame, columns: [str], raise_exception=False):
    try:
        data_frame.columns
    except NameError:
        if raise_exception:
            raise Exception(
                f'The DataFrame does not have columns:{data_frame}')
        else:
            return False
    for _column in columns:
        if _column not in data_frame.columns:
            if raise_exception:
                raise Exception(
                    f'The DataFrame expected to contain {_column} but have these columns:{data_frame.columns}')
            else:
                return False
    return True


def read_ohlca(date_range_string: str) -> pd.DataFrame:
    # try:
    #     ohlca = pd.read_csv(f'ohlca.{date_range_string}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])
    # except pd.errors.ParserError as e:
    #     convert_to_ohlca_csv(f'ohlc.{date_range_string}.zip')
    #     ohlca = pd.read_csv(f'ohlca.{date_range_string}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])
    #
    # return ohlca
    return read_file(date_range_string, 'ohlca')


def read_ohlc(date_range_string: str) -> pd.DataFrame:
    try:
        return read_file(date_range_string, 'ohlc')
    except:
        log(f'Failed to load ohlc.{date_range_string} try to load ohlca.{date_range_string}')
        return read_file(date_range_string, 'ohlc')
