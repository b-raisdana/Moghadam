from datetime import datetime

import pandas as pd

from BullBearSide import read_multi_timeframe_trend_boundaries, generate_multi_timeframe_trend_boundaries
from Config import config
from DataPreparation import to_timeframe

if __name__ == "__main__":
    # multi_timeframe_ohlca = read_multi_timeframe_ohlca(config.under_process_date_range)
    # timeframe = '1min'
    # plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe), name=f'{timeframe} ohlca')
    # # timeframe = '5min'
    # # plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe), name=f'{timeframe} ohlca')
    # # timeframe = '15min'
    # # plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe), name=f'{timeframe} ohlca')
    # # timeframe = '1H'
    # # plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe), name=f'{timeframe} ohlca')
    # # timeframe = '4H'
    # plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe), name=f'{timeframe} ohlca')
    # generate_multi_timeframe_ohlca(config.under_process_date_range)
    generate_multi_timeframe_trend_boundaries(config.under_process_date_range, timeframe_short_list=['15min'])
    boundaries = read_multi_timeframe_trend_boundaries(config.under_process_date_range)
    exit(0)
