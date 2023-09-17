from __future__ import annotations

import struct
import time
import zipfile
from os import path, remove
from typing import Union

import MetaTrader5 as Mt5
from pandera import typing as pt

from DataPreparation import map_symbol, extract_file_info, FileInfoSet
from Model.MultiTimeframeOHLC import OHLCV

META_TRADER_IS_INITIALIZED = False


def mt5_client() -> Mt5:
    global META_TRADER_IS_INITIALIZED
    if not META_TRADER_IS_INITIALIZED:
        if not Mt5.initialize():
            print("initialize() failed, error code =", Mt5.last_error())
            quit()
        META_TRADER_IS_INITIALIZED = True
    return Mt5


_meta_trader_symbol_map = {
    'BTC/USDT': 'BTCUSDT',
}


def map_to_meta_trader_symbol(symbol: str) -> str:
    return map_symbol(symbol, _meta_trader_symbol_map)


class MT:
    singleton: MT

    @staticmethod
    def data_path():
        terminal_info = mt5_client().terminal_info()
        return terminal_info.data_path

    @staticmethod
    def copy_to_data_path(source_file_path: str, make_dir: bool = False,
                          do_not_raise_exception: bool = False) -> Union[bool, None]:
        if source_file_path[:-4] != '.zip':
            raise Exception('source_file_path should end with .zip but:' + source_file_path)
        source_file_name = path.basename(source_file_path)
        source_file_extension_striped = source_file_name[:-4]
        source_file_info: FileInfoSet = extract_file_info(source_file_name)
        destination_file_name = map_to_meta_trader_symbol(source_file_info.symbol) + source_file_info.file_type + '.csv'
        destination_folder = MT.data_path()
        destination_path = path.join(destination_folder, destination_file_name)
        if path.exists(destination_path):
            remove(destination_path)

        if make_dir or do_not_raise_exception:
            raise Exception('Not implemented');
        with zipfile.ZipFile(source_file_path, 'r') as zip:
            zip.extract(source_file_extension_striped, destination_path)
        return


def zz_write_hst(df: pt.DataFrame[OHLCV], symbol: str, period: int, price_precision: int, filepath: str):
    # Define the structures as per the MT4 HST format
    HST_HEADER_FORMAT = '64s12si2i13i'
    HST_RECORD_FORMAT = 'I5d'
    # Construct the header data
    header = struct.pack(HST_HEADER_FORMAT,
                         b'MetaTrader 4 Copyright',  # copyright
                         symbol.encode('ascii'),  # symbol
                         period,  # period
                         price_precision,  # digits
                         int(time.time()),  # timesign
                         int(time.time()),  # last_sync
                         *([0] * 12)  # unused
                         )
    with open(filepath, 'wb') as f:
        f.write(header)

        for index, row in df.iterrows():
            record = struct.pack(HST_RECORD_FORMAT,
                                 int(index.timestamp()),
                                 row['open'],
                                 row['low'],
                                 row['high'],
                                 row['close'],
                                 row['volume']
                                 )
            f.write(record)
