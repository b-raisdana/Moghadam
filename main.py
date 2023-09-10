from sys import exit

from BullBearSide import generate_multi_timeframe_bull_bear_side_trends
from BullBearSidePivot import generate_multi_timeframe_bull_bear_side_pivots
from Config import config
from fetch_ohlcv import under_process_date_range

if __name__ == "__main__":
    config.under_process_date_range = under_process_date_range()

    # t1 = seven_days_before_dataframe()
    # pprint(t1)

    # generate_multi_timeframe_peaks_n_valleys(config.under_process_date_range)
    # generate_multi_timeframe_candle_trend(config.under_process_date_range)
    # generate_multi_timeframe_bull_bear_side_trends(config.under_process_date_range, timeframe_shortlist=['4H'])
    generate_multi_timeframe_bull_bear_side_pivots(config.under_process_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_top_pivots(config.under_process_date_range)  # , timeframe_shortlist=['15min'])
    exit()
