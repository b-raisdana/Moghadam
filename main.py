import os
from datetime import timedelta
from sys import exit
from time import sleep

from Candle import generate_ohlcv, read_ohlcv
from Config import config
from MetaTrader import MT
from helper import date_range_to_string, log
from helper import today_morning

if __name__ == "__main__":
    config.under_process_date_range = date_range_to_string(days=60)

    config.load_data_to_meta_trader = False
    file_path: str = config.path_of_data

    today_morning = today_morning()
    for month in range(6,8):
        date_range_str = date_range_to_string(days = 30, end_date=today_morning - timedelta(days=30 * month))
        log(f'date_range_str{date_range_str}')
        ohlcv = read_ohlcv(date_range_str)
        ohlcv = ohlcv[['open', 'high', 'low', 'close', 'volume']]
        ohlcv.to_csv(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'),
              compression='zip')
        MT.extract_to_data_path(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'))
        MT.load_rates()
        sleep(30)

    exit(0)
    # date_range_str = config.under_process_date_range
    # file_path: str = config.path_of_data
    # MT.extract_to_data_path(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'))
    # MT.load_rates()
    # exit(0)

    # generatePeaks_n_Valleys
    # _ohlcv()
    # generate_multi_timeframe_peaks_n_valleys(config.under_process_date_range)
    # generate_multi_timeframe_candle_trend(config.under_process_date_range)
    # generate_multi_timeframe_bull_bear_side_trends(config.under_process_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_bull_bear_side_pivots(config.under_process_date_range, timeframe_shortlist=['4H'])
    generate_multi_timeframe_top_pivots(config.under_process_date_range)  # , timeframe_shortlist=['15min'])


    exit()
