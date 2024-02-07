import pandas as pd

from ClassicPivot import generate_multi_timeframe_pivots
from helper.helper import measure_time, log


@measure_time
def generate_multi_timeframe_gap_levels():
    """
        FTR:
        todo: algorithm of FTR gaps
        FTC:
        todo: algorithm of FTC gaps
        check long candles after last found gap:
            for long candles passing or forming a level:
                the most significant tie inside long candle is a gap:
                    most significant tie:
                        switch time to pattern recursively to find SIDE trends inside candle time trend:
                            the SIDE with maximum movement is the Tie???
        todo: algorithm of SO4 gpas
        todo: algorithm of ISLAND gaps
        todo: algorithm of PERSIAN GOLF gaps
    :return:
    """
    raise Exception('Not implemented')


@measure_time
def generate_multi_timeframe_pivots(multi_timeframe_ohlcva: pd.DataFrame, multi_timeframe_peaks_n_valleys: pd.DataFrame):
    log("Not finished!")
    generate_multi_timeframe_pivots()
    generate_multi_timeframe_gap_levels()

    # todo: add gap_level_pivots
    raise Exception('Not implemented')
