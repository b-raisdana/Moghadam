"""
I have
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

# Import the backtrader platform
import backtrader as bt

from Config import config
from ohlcv import read_base_timeframe_ohlcv


# class PandasData(bt.feeds.DataBase):
#     params = (
#         ('datetime', 'date'),
#         ('open', 'open'),
#         ('high', 'high'),
#         ('low', 'low'),
#         ('close', 'close'),
#         ('volume', 'volume'),
#         ('openinterest', -1),
#     )


class BasePatternStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close

    def next(self):
        if self.
        self.log('Close, %.2f' % self.dataclose[0])


def test_strategy():
    cerebro = bt.Cerebro()
    cerebro.addstrategy(BasePatternStrategy)
    # config.processing_date_range = '23-12-25.00-00T23-12-26.23-59'
    raw_data = read_base_timeframe_ohlcv(config.processing_date_range)
    data = bt.feeds.PandasData(dataname=raw_data, datetime=None, open=0, close=1, high=2, low=3, volume=4, openinterest=-1)
    # PandasData(dataname=raw_data)
    cerebro.adddata(data)
    cerebro.broker.setcash(100.0)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
