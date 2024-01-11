from BasePattern import read_multi_timeframe_base_patterns
from BullBearSide import read_multi_timeframe_bull_bear_side_trends
from Config import config
from FigurePlotter.BasePattern_plotter import plot_multi_timeframe_base_pattern
from FigurePlotter.BullBearSide_plotter import plot_multi_timeframe_bull_bear_side_trends
from FigurePlotter.OHLVC_plotter import plot_multi_timeframe_ohlcv, plot_multi_timeframe_ohlcva
from FigurePlotter.PeakValley_plotter import plot_multi_timeframe_peaks_n_valleys
from PeakValley import read_multi_timeframe_peaks_n_valleys
from Strategy.BasePatternStrategy import test_strategy
from atr import read_multi_timeframe_ohlcva
from helper.helper import date_range_to_string
from ohlcv import read_multi_timeframe_ohlcv

# from data_preparation import d_types

if __name__ == "__main__":
    config.processing_date_range = date_range_to_string(days=4)
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

    # _ohlcv = read_multi_timeframe_ohlcv(config.processing_date_range)
    # plot_multi_timeframe_ohlcv(_ohlcv, config.processing_date_range, show=True)
    # ohlcva = read_multi_timeframe_ohlcva()
    # plot_multi_timeframe_ohlcva(ohlcva, show=False)
    # _peaks_and_valleys = read_multi_timeframe_peaks_n_valleys()
    # plot_multi_timeframe_peaks_n_valleys(_peaks_and_valleys, config.processing_date_range)
    # bull_bear_side = read_multi_timeframe_bull_bear_side_trends()
    # plot_multi_timeframe_bull_bear_side_trends(ohlcva, _peaks_and_valleys, bull_bear_side,
    #                                            timeframe_shortlist=['4H', '1D', '1W'])
    # exit()

    # generate_multi_timeframe_bull_bear_side_pivots()
    # _bull_bear_side_pivots = read_multi_timeframe_bull_bear_side_pivots()
    # plot_multi_timeframe_pivots(_bull_bear_side_pivots)

    # generate_multi_timeframe_base_patterns()
    # _base_patterns = read_multi_timeframe_base_patterns()
    # _base_patterns = _base_patterns[~_base_patterns['ignore_backtesting']]
    # plot_multi_timeframe_base_pattern(ohlcva, _base_patterns)
    # exit(0)
    test_strategy(cash=100000)
