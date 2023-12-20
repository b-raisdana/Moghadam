from sys import exit

from BullBearSide import generate_multi_timeframe_bull_bear_side_trends, read_multi_timeframe_bull_bear_side_trends, \
    multi_timeframe_bull_bear_side_trends, generate_multi_timeframe_candle_trend
from ClassicPivot import read_pivots
from Config import config
from FigurePlotter.BullBearSide_plotter import plot_multi_timeframe_bull_bear_side_trends
from PeakValley import read_multi_timeframe_peaks_n_valleys
from atr import generate_multi_timeframe_ohlcva, read_multi_timeframe_ohlcva
from helper import date_range_to_string

if __name__ == "__main__":
    config.processing_date_range = date_range_to_string(days=1)
    #
    #     file_path: str = config.path_of_data
    #     today_morning = today_morning()
    #     for month in range(0, 2):
    #         date_range_str = date_range_to_string(days=30, end=today_morning - timedelta(days=30 * month))
    #         log(f'date_range_str{date_range_str}', stack_trace=False)
    #         ohlcv = read_base_timeframe_ohlcv(date_range_str)
    #         ohlcv = ohlcv[['open', 'high', 'low', 'close', 'volume']]
    #         ohlcv.to_csv(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'),
    #                      compression='zip')
    #         MT.extract_to_data_path(os.path.join(file_path, f'ohlcv.{date_range_str}.zip'))
    #         MT.load_rates()
    #         # sleep(30)
    #
    #     exit(0)

    # t = empty_df(PeakValleys)
    # t = empty_df(MultiTimeframePeakValleys)

    # generate_multi_timeframe_ohlcv('23-09-04.00-00T23-12-18.23-59')
    # # generate_multi_timeframe_ohlcv(config.processing_date_range)
    # _ohlcv = read_multi_timeframe_ohlcv('23-09-04.00-00T23-12-18.23-59')
    # t = single_timeframe(_ohlcv, '1W')
    # plot_multi_timeframe_ohlcv(_ohlcv, config.processing_date_range, show=True)
    # exit()

    # generate_multi_timeframe_ohlcva()
    ohlcva = read_multi_timeframe_ohlcva()
    # plot_multi_timeframe_ohlcva(_ohlcva, show=False)

    # _peaks_and_valleys = multi_timeframe_peaks_n_valleys(config.processing_date_range)
    # generate_multi_timeframe_peaks_n_valleys(config.processing_date_range)  # config.processing_date_range)
    _peaks_and_valleys = read_multi_timeframe_peaks_n_valleys()
    # plot_multi_timeframe_peaks_n_valleys(_peaks_and_valleys, config.processing_date_range)
    generate_multi_timeframe_candle_trend(config.processing_date_range)
    generate_multi_timeframe_bull_bear_side_trends()
    bull_bear_side = read_multi_timeframe_bull_bear_side_trends()
    # bull_bear_side = read_multi_timeframe_bull_bear_side_trends()
    plot_multi_timeframe_bull_bear_side_trends(ohlcva, _peaks_and_valleys, bull_bear_side)
    # pivots = read_pivots(config.processing_date_range)

    exit()
