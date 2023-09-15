from datetime import timedelta
from pprint import pprint
from sys import exit

import pandas as pd

from BullBearSide import generate_multi_timeframe_bull_bear_side_trends
from BullBearSidePivot import generate_multi_timeframe_bull_bear_side_pivots
from Candle import read_multi_timeframe_ohlc
from ClassicPivot import generate_multi_timeframe_top_pivots
from Config import config
from DataPreparation import single_timeframe
from fetch_ohlcv import under_process_date_range
from MetaTrader import get_data_path

if __name__ == "__main__":
    config.under_process_date_range = under_process_date_range()

    data_path = get_data_path()
    # multi_timeframe_ohlc = read_multi_timeframe_ohlc()
    # timeframe_ohlc = single_timeframe(multi_timeframe_ohlc, '15min')
    # print(timeframe_ohlc)
    # pprint(timeframe_ohlc)
    # write_hst(timeframe_ohlc, 'CustomBTCUSD',
    #           period=int(pd.to_timedelta('15min').total_seconds()),
    #           price_precision=2,
    #           filepath='./CustomSymbol-BTCUSD.hst')
    # t1 = seven_days_before_dataframe()
    # pprint(t1)

    # generate_multi_timeframe_peaks_n_valleys(config.under_process_date_range)
    # generate_multi_timeframe_candle_trend(config.under_process_date_range)
    # generate_multi_timeframe_bull_bear_side_trends(config.under_process_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_bull_bear_side_pivots(config.under_process_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_top_pivots(config.under_process_date_range)  # , timeframe_shortlist=['15min'])
    exit()
