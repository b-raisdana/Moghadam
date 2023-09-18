from __future__ import annotations

import os
import struct
import subprocess
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
    def extract_to_data_path(source_file_path: str, make_dir: bool = False,
                             do_not_raise_exception: bool = False) -> Union[bool, None]:
        if source_file_path[-4:] != '.zip':
            raise Exception('source_file_path should end with .zip but:' + source_file_path)
        source_file_name = path.basename(source_file_path)
        source_file_extension_striped = source_file_name[:-4]
        source_file_info: FileInfoSet = extract_file_info(source_file_name)
        # destination_file_name = (map_to_meta_trader_symbol(source_file_info['symbol']) + '.' +
        #                          source_file_info['file_type'] +
        #                          '.csv')
        files_path_sub_folder = 'MQL5\Files'
        destination_folder = path.join(MT.data_path(), files_path_sub_folder)
        # destination_path = path.join(destination_folder, files_path_sub_folder) # , destination_file_name)
        if make_dir or do_not_raise_exception:
            raise Exception('Not implemented');
        with zipfile.ZipFile(source_file_path, 'r') as zip:
            for file in zip.filelist:
                if path.exists(path.join(destination_folder, file.filename)):
                    remove(path.join(destination_folder, file.filename))
                _destination_file = path.join(destination_folder, 'Custom' + source_file_info.symbol + '.' +
                                              source_file_info.file_type + '.csv')
                if path.exists(_destination_file):
                    remove(_destination_file)
            zip.extract(source_file_extension_striped, destination_folder, )
            for file in zip.filelist:
                if file.filename[:len(source_file_info.symbol)] != source_file_info.symbol:
                    _source_file = path.join(destination_folder, file.filename)
                    # _destination_file = source_file_info['symbol'] + '.' + _source_file
                    _destination_file = path.join(destination_folder, 'Custom' + source_file_info.symbol + '.' +
                                                  source_file_info.file_type + '.csv')
                    os.rename(_source_file, _destination_file)
        return


def run_by_autoit():
    relative_path_of_CustomProfile_chart_config_file_inder_data_folder = "\MQL5\Profiles\Charts\CustomProfile"
    name_of_CustomProfile_chart_config_file = "chart01.chr"
    content_of_CustomProfile_chart_config_file = """
    <chart>
id=133394933231445677
symbol=CustomBTCUSDT
description=
period_type=1
period_size=1
digits=4
tick_size=0.000000
position_time=1694491200
scale_fix=0
scale_fixed_min=25460.738000
scale_fixed_max=26884.364000
scale_fix11=0
scale_bar=0
scale_bar_val=1.000000
scale=16
mode=1
fore=0
grid=1
volume=2
scroll=1
shift=0
shift_size=19.832985
fixed_pos=0.000000
ticker=1
ohlc=0
one_click=0
one_click_btn=1
bidline=1
askline=0
lastline=0
days=0
descriptions=0
tradelines=1
tradehistory=1
window_left=0
window_top=0
window_right=1503
window_bottom=658
window_type=1
floating=0
floating_left=0
floating_top=0
floating_right=0
floating_bottom=0
floating_type=1
floating_toolbar=1
floating_tbstate=
background_color=0
foreground_color=16777215
barup_color=65280
bardown_color=65280
bullcandle_color=0
bearcandle_color=16777215
chartline_color=65280
volumes_color=3329330
grid_color=10061943
bidline_color=10061943
askline_color=255
lastline_color=49152
stops_color=255
windows_total=1

<expert>
name=CustomAutoLoader
path=Experts\CustomAutoLoader.ex5
expertmode=33
<inputs>
</inputs>
</expert>

<window>
height=100.000000
objects=0

<indicator>
name=Main
path=
apply=1
show_data=1
scale_inherit=0
scale_line=0
scale_line_percent=50
scale_line_value=0.000000
scale_fix_min=0
scale_fix_min_val=0.000000
scale_fix_max=0
scale_fix_max_val=0.000000
expertmode=0
fixed_height=-1
</indicator>
</window>
</chart>
    """
    # Specify the path to the AutoIt executable
    autoit_executable_path = "C:\Program Files (x86)\AutoIt3\AutoIt3_x64.exe"

    # Specify the path to your AutoIt script
    autoit_script_path = "load_meta_trader.au3"

    # Use subprocess to run the script
    result = subprocess.run([autoit_executable_path, autoit_script_path], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    # Print the result
    print("STDOUT:", result.stdout.decode('utf-8'))
    print("STDERR:", result.stderr.decode('utf-8'))


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
