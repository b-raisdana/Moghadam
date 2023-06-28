import pandas as pd
import talib as ta

from Config import config


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


def read_ohlca(date_range_string: str) -> pd.DataFrame:
    try:
        ohlca = pd.read_csv(f'ohlca.{date_range_string}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])
    except pd.errors.ParserError as e:
        convert_to_ohlca_csv(f'ohlc.{date_range_string}.zip')
        ohlca = pd.read_csv(f'ohlca.{date_range_string}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])

    return ohlca
