import sys
from datetime import datetime

from AtrMovementPivots import atr_movement_pivots, read_multi_timeframe_atr_movement_pivots, \
    generate_multi_timeframe_atr_movement_pivots
from BasePattern import read_multi_timeframe_base_patterns
from BullBearSide import read_multi_timeframe_bull_bear_side_trends
from Config import config
from FigurePlotter.BasePattern_plotter import plot_multi_timeframe_base_pattern
from FigurePlotter.Pivot_plotter import plot_multi_timeframe_pivots
from atr import read_multi_timeframe_ohlcva
from ftc import multi_timeframe_ftc
from helper.helper import date_range_to_string
from ohlcv import read_multi_timeframe_ohlcv

# from data_preparation import d_types

if __name__ == "__main__":
    # config.processing_date_range = date_range_to_string(days=5, end=datetime(year=2023, month=11, day=18))
    config.processing_date_range = date_range_to_string(days=2, end=datetime(year=2023, month=11, day=18))
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

    ohlcva = read_multi_timeframe_ohlcva(config.processing_date_range)
    # _peaks_and_valleys = read_multi_timeframe_peaks_n_valleys()
    # # plot_multi_timeframe_peaks_n_valleys(_peaks_and_valleys, config.processing_date_range)
    # # exit(0)
    # bull_bear_side = read_multi_timeframe_bull_bear_side_trends()
    # plot_multi_timeframe_bull_bear_side_trends(ohlcva, _peaks_and_valleys, bull_bear_side)
    # generate_multi_timeframe_atr_movement_pivots(config.processing_date_range)
    pivots = read_multi_timeframe_atr_movement_pivots(config.processing_date_range)
    major_pivots = pivots[pivots['major_timeframe']]
    # #
    # # generate_multi_timeframe_bull_bear_side_pivots(config.processing_date_range)
    # # _pivots = read_multi_timeframe_bull_bear_side_pivots(config.processing_date_range)
    trends = read_multi_timeframe_bull_bear_side_trends(config.processing_date_range)
    # exit(0)
    # generate_multi_timeframe_base_patterns()

    base_patterns = read_multi_timeframe_base_patterns()
    # # orders_df = pd.read_csv(
    # #     os.path.join(config.path_of_data,
    # #                  f'BasePatternStrategy.orders.0Rzb5KJmWrXfRsnjTE1t9g.24-01-11.00-00T24-01-12.23-59.csv'))
    # plot_multi_timeframe_base_pattern(base_patterns, ohlcva)  # , orders_df=orders_df)


    # plot_multi_timeframe_pivots(major_pivots[major_pivots['major_timeframe'].astype(bool)], group_by='timeframe')
    multi_timeframe_ftc(
        mt_pivot=major_pivots,
        mt_frame_bbs_trend=trends,
        mt_ohlcv = ohlcva,
        multi_timeframe_base_patterns = base_patterns,
        )
    sys.exit(0)
    test_strategy(cash=100000)
