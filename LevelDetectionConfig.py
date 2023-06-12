import datetime
import json
import hashlib
import base64

from pandas._libs.tslibs import to_offset


class Config():
    def __init__(self):
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
        self.feature_columns = ['open', 'high', 'low', 'close', 'volume']

        self.end_time = '2021-03-01 03:43:00'

        self.id = datetime.datetime.now().strftime("%Y-%m-%d.%H-%M")


class MyEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


myEncoder = MyEncoder()
debug = False
config = Config()
print(myEncoder.encode(config))
print(hashlib.md5(myEncoder.encode(config).encode('utf-8')))
print(base64.b64encode(hashlib.md5(myEncoder.encode(config).encode('utf-8')).digest()))
with open(f'LevelDetection.{config.id}.config.txt', 'w') as config_file:
    config_file.write(myEncoder.encode(config))
