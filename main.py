from BullBearSide import generate_multi_timeframe_bull_bear_side_trends
from BullBearSidePivot import generate_multi_timeframe_bull_bear_side_pivots
from Config import config

if __name__ == "__main__":
    # generate_multi_timeframe_peaks_n_valleys(config.under_process_date_range)
    # generate_multi_timeframe_candle_trend(config.under_process_date_range)
    # generate_multi_timeframe_bull_bear_side_trends(config.under_process_date_range) #, timeframe_short_list=['15min'])
    generate_multi_timeframe_bull_bear_side_pivots(config.under_process_date_range) #, timeframe_short_list=['15min'])
    exit(0)
