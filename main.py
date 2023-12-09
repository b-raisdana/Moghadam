import os
from datetime import timedelta
from sys import exit

import pandas as pd

from Config import config
from Model.MultiTimeframePeakValleys import PeakValleys, MultiTimeframePeakValleys
from data_preparation import expand_date_range, empty_df
from FigurePlotter.OHLVC_plotter import plot_multi_timeframe_ohlcva, plot_multi_timeframe_ohlcv
from FigurePlotter.PeakValley_plotter import plot_multi_timeframe_peaks_n_valleys
from MetaTrader import MT
from Model.MultiTimeframeOHLCV import MultiTimeframeOHLCV, OHLCV
from PeakValley import read_multi_timeframe_peaks_n_valleys, multi_timeframe_peaks_n_valleys
from atr import generate_multi_timeframe_ohlcva, read_multi_timeframe_ohlcva
from helper import date_range_to_string, log
from helper import today_morning
from ohlcv import read_base_timeframe_ohlcv, generate_multi_timeframe_ohlcv, read_multi_timeframe_ohlcv

if __name__ == "__main__":
    config.processing_date_range = date_range_to_string(days=60)
    #
    # file_path: str = config.path_of_data
    # today_morning = today_morning()
    # for month in range(0, 2):
    #     date_range_str = date_range_to_string(days=30, end=today_morning - timedelta(days=30 * month))
    #     log(f'date_range_str{date_range_str}')
    #     ohlcv = read_base_timeframe_ohlcv(date_range_str)
    #     ohlcv = ohlcv[['open', 'high', 'low', 'close', 'volume']]
    #     ohlcv.to_csv(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'),
    #                  compression='zip')
    #     MT.extract_to_data_path(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'))
    #     MT.load_rates()
    #     # sleep(30)
    #
    # exit(0)

    # t = empty_df(PeakValleys)
    # t = empty_df(MultiTimeframePeakValleys)

    # generate_multi_timeframe_ohlcv(config.processing_date_range)
    # _ohlcv = read_multi_timeframe_ohlcv(config.processing_date_range)
    # # plot_multi_timeframe_ohlcv(_ohlcv, config.processing_date_range, show=False)

    # generate_multi_timeframe_ohlcva(config.processing_date_range)
    # _ohlcva = read_multi_timeframe_ohlcva(config.processing_date_range)
    # # plot_multi_timeframe_ohlcva(_ohlcva, show=False)

    _peaks_and_valleys = read_multi_timeframe_peaks_n_valleys(config.processing_date_range)
    plot_multi_timeframe_peaks_n_valleys(_peaks_and_valleys, config.processing_date_range)
    # generate_multi_timeframe_candle_trend(config.processing_date_range)
    # generate_multi_timeframe_bull_bear_side_trends(config.processing_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_bull_bear_side_pivots(config.processing_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_top_pivots(config.processing_date_range)  # , timeframe_shortlist=['15min'])

    exit()
