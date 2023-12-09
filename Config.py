import base64
import hashlib
import json
import os
import sys
from datetime import timedelta
from enum import Enum

import pandas as pd

from helper import log


class CandleSize(Enum):
    Spinning = (0.0, 0.80)
    Standard = (0.80, 1.20)
    Long = (1.20, 2.5)
    Spike = (2.5, 999999999)


class TREND(Enum):
    BULLISH = 'BULLISH_TREND'
    BEARISH = 'BEARISH_TREND'
    SIDE = 'SIDE_TREND'


class TopTYPE(Enum):
    PEAK = 'peak'
    VALLEY = 'valley'


GLOBAL_CACHE = {}


class Config():
    def __init__(self):
        # self.processing_date_range = '17-12-24.00-00T17-12-31.23-59'
        self.processing_date_range = '17-12-01.00-00T17-12-31.23-59'
        self.limit_to_under_process_period = True
        self.under_process_symbol = 'BTCUSDT'
        self.files_to_load = [
            '17-01-01.0-01TO17-12-31.23-59.1min',
            '17-01-01.0-01TO17-12-31.23-59.5min',
            '17-01-01.0-01TO17-12-31.23-59.15min',
            '17-01-01.0-01TO17-12-31.23-59.1h',
            '17-01-01.0-01TO17-12-31.23-59.4h',
            '17-01-01.0-01TO17-12-31.23-59.1D',
            '17-01-01.0-01TO17-12-31.23-59.1W',
        ]
        self.data_path_preamble = 'https://raw.githubusercontent.com/b-raisdana/BTC-price/main/'

        self.timeframe_shifter = {
            'structure': 0,
            'pattern': -1,
            'trigger': -2,
            'double': -4,
            'hat_trick': -6,
        }
        self.timeframes = [
            '1min',  #: to_offset('1min'),
            '5min',  #: to_offset('5min'),
            '15min',  #: to_offset('15min'),
            '1H',  #: to_offset('1H'),
            '4H',  #: to_offset('4H'),
            '1D',  #: to_offset('1D'),
            '1W',  #: to_offset('1W')
        ]
        self.structure_timeframes = self.timeframes[2:]
        self.pattern_timeframes = self.timeframes[1:]
        self.trigger_timeframes = self.timeframes[:-2]
        self.hat_trick_index = 0
        self.trigger_dept = 16

        self.dept_of_analysis = 3

        self.end_time = '2021-03-01 03:43:00'

        self.INFINITY_TIME_DELTA = timedelta(days=10 * 365)

        self.path_of_data = 'data'
        self.path_of_plots = os.path.join(self.path_of_data, 'plots')
        self.path_of_logs = 'logs'
        self.path_of_test_plots = os.path.join('test_plots')

        self.base_time_delta = pd.to_timedelta(self.timeframes[0])  # timedelta(minutes=1)

        self.momentum_trand_strength_factor = 0.70  # CandleSize.Standard.value[0]

        self.load_data_to_meta_trader = False

        self.ATR_timeperiod = 14

        self.id = ""


class MyEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            return o.__dict__
        except AttributeError:
            return str(o)
        except Exception as e:
            exception_info = sys.exc_info()
            raise


myEncoder = MyEncoder()
DEBUG = False
config = Config()
config_as_json = myEncoder.encode(config)
if DEBUG: log(str(config_as_json))
config_digest = str.translate(base64.b64encode(hashlib.md5(config_as_json.encode('utf-8')).digest())
                              .decode('ascii'), {ord('+'): '', ord('/'): '', ord('='): '', })

if DEBUG: log(config_digest)
dump_filename = os.path.join(config.path_of_logs, f'Config.{config_digest}.json')
if not os.path.exists(config.path_of_logs):
    os.makedirs(config.path_of_logs)
if not os.path.exists(dump_filename):
    with open(dump_filename, 'w+') as config_file:
        config_file.write(str(config_as_json))

config.id = config_digest

INFINITY_TIME_DELTA = config.INFINITY_TIME_DELTA
