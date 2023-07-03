import base64
import hashlib
import json
from datetime import timedelta
from enum import Enum


class Config():
    def __init__(self):
        self.under_process_date_range = config.under_process_date_range,
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

        self.time_shifter = {
            'structure': 0,
            'pattern': -1,
            'trigger': -2,
            'double': -4,
            'hat_trick': -6,
        }
        self.times = [
            '1min',  #: to_offset('1min'),
            '5min',  #: to_offset('5min'),
            '15min',  #: to_offset('15min'),
            '1H',  #: to_offset('1H'),
            '4H',  #: to_offset('4H'),
            '1D',  #: to_offset('1D'),
            '1W',  #: to_offset('1W')
        ]
        self.hat_trick_index = 0
        self.trigger_dept = 16

        self.dept_of_analysis = 3
        self.ohlc_columns = ['open', 'high', 'low', 'close', 'volume']
        self.ohlca_columns = self.ohlc_columns.append('ATR')
        self.multi_time_ohlca_columns = self.ohlca_columns.append('effective_time')
        self.multi_time_peaks_n_valleys_columns = self.ohlc_columns.append(
            ['effective_time', 'strength', 'peak_or_valley'])

        self.end_time = '2021-03-01 03:43:00'

        self.INFINITY_TIME_DELTA = timedelta(days=10 * 365)

        self.id = ""


class MyEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            return o.__dict__
        except AttributeError:
            return str(o)


myEncoder = MyEncoder()
DEBUG = False
config = Config()
config_as_json = myEncoder.encode(config).encode('utf-8')
if DEBUG: print(str(config_as_json))
config_digest = str.translate(base64.b64encode(hashlib.md5(config_as_json).digest())
                              .decode('ascii'), {ord('+'): '', ord('/'): '', ord('='): '', })

if DEBUG: print(config_digest)
with open(f'Config.{config_digest}.txt', 'w+') as config_file:
    config_file.write(str(config_as_json))

config.id = config_digest

INFINITY_TIME_DELTA = config.INFINITY_TIME_DELTA


class TREND(Enum):
    BULLISH = 'BULLISH_TREND'
    BEARISH = 'BEARISH_TREND'
    SIDE = 'SIDE_TREND'


class TopTYPE(Enum):
    PEAK = 'peak'
    VALLEY = 'valley'
