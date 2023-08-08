from BullBearSide import read_multi_timeframe_trend_boundaries, generate_multi_timeframe_trend_boundaries
from Config import config, GLOBAL_CACHE
from DataPreparation import read_multi_timeframe_ohlc, single_timeframe, read_multi_timeframe_ohlca, plot_ohlca

if __name__ == "__main__":
    # mult_timeframe_ohlca = read_multi_timeframe_ohlca(config.under_process_date_range)
    # plot_ohlca(single_timeframe(mult_timeframe_ohlca, '15min'))
    # exit(0)
    mult_timeframe_ohlc = read_multi_timeframe_ohlc(config.under_process_date_range)
    for timeframe in config.timeframes:
        GLOBAL_CACHE[f'ohlc_{timeframe}'] = \
            single_timeframe(mult_timeframe_ohlc, timeframe).index.get_level_values('date').tolist()
    generate_multi_timeframe_trend_boundaries(config.under_process_date_range , timeframe_short_list=['15min'])
    boundaries = read_multi_timeframe_trend_boundaries(config.under_process_date_range)
    exit(0)
