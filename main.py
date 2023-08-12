from BullBearSide import read_multi_timeframe_trend_boundaries, generate_multi_timeframe_trend_boundaries
from Config import config, GLOBAL_CACHE
from DataPreparation import read_multi_timeframe_ohlc, single_timeframe, read_multi_timeframe_ohlca, plot_ohlca
from PeaksValleys import read_multi_timeframe_peaks_n_valleys
from SupportResistanceLevels import active_tops

if __name__ == "__main__":
    # mult_timeframe_ohlc = read_multi_timeframe_ohlc(config.under_process_date_range)
    # for timeframe in config.timeframes:
    #     GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
    #         single_timeframe(mult_timeframe_ohlc, timeframe).index.get_level_values('date').tolist()
    # generate_multi_timeframe_trend_boundaries(config.under_process_date_range , timeframe_short_list=['15min'])
    # boundaries = read_multi_timeframe_trend_boundaries(config.under_process_date_range)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str=config.under_process_date_range)
    active_tops(multi_timeframe_peaks_n_valleys)
    exit(0)
