import datetime
import json


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
            'hattrick': -6,
            }

        self.times = [(1, '1min'), (5, '5min'), (15, '15min'), (60, '1H'), (240, '4H'), (60*24, '1D'), (60*24*7, '1W')]
        self.hattrick_index = 0
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

with open(f'LevelDetection.{config.id}.config.txt', 'w') as config_file:
    config_file.write(myEncoder.encode(config))
