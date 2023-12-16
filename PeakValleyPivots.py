import os

import pandas as pd
from pandera import typing as pt

from Config import config
from data_preparation import single_timeframe, anti_trigger_timeframe, cast_and_validate, \
    read_file, after_under_process_date, empty_df
from MetaTrader import MT
from Model.Pivot import MultiTimeframePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys
from PivotsHelper import pivots_level_n_margins, level_ttl
from atr import read_multi_timeframe_ohlcva
from helper import measure_time


def tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivot]:
    """

    :param date_range_str:
    :return:
    """
    '''
    A top (Peak or Valley) have significant impact of price movement in the Trigger Timeframe. for example, 1D Peaks and
    Valleys are not forcing 1D price chart but they impact 1H chart and price in 1H chart react to 1D top levels hit.
    As a result:
        1. 1W tops are creating classic levels for 4H, 1D for 1H and 4H for 15min structure Timeframes.
        2. we use the 1H chart for 1D tops because they are creating 1H classic levels.
    '''
    _multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    _multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)
    multi_timeframe_pivots = empty_df(MultiTimeframePivot)
    for timeframe in config.structure_timeframes[::-1][2:]:
        # 1W tops are creating classic levels for 4H, 1D for 1H and 4H for 15min structure Timeframes.
        _pivots = single_timeframe(_multi_timeframe_peaks_n_valleys, anti_trigger_timeframe(timeframe))
        ohlcv_start = _multi_timeframe_ohlcva.index.get_level_values('date').min()
        '''
        first part of the chart with the length of anti_trigger_timeframe(timeframe) is not reliable. We have to now 
        about anti_trigger_timeframe(timeframe) to make sure the detected Top is not for a anti-trigger Timeframe.  
        '''
        _pivots = _pivots.loc[ohlcv_start + pd.to_timedelta(anti_trigger_timeframe(timeframe)):]
        anti_trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, anti_trigger_timeframe(timeframe))
        '''
        we use the 1H chart for 1D tops because they are creating 1H classic levels.
        '''
        timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)
        trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)  # trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(pivot_peaks_or_valleys=_pivots, timeframe_pivots=_pivots,
                                         # timeframe=anti_trigger_timeframe(timeframe),
                                         timeframe=timeframe,
                                         candle_body_source=timeframe_ohlcva,
                                         internal_atr_source=timeframe_ohlcva,
                                         breakout_atr_source=trigger_timeframe_ohlcva,
                                         )
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0  # update_hits(multi_timeframe_pivots)
        _pivots['is_overlap_of'] = None
        _pivots['deactivated_at'] = None
        _pivots['archived_at'] = None

        if len(_pivots) > 0:
            # _pivots = cast_and_validate(_pivots, Pivot)
            # _pivots['timeframe'] = anti_pattern_timeframe(timeframe)
            _pivots['timeframe'] = timeframe
            _pivots.set_index('timeframe', append=True, inplace=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = pd.concat([multi_timeframe_pivots, _pivots])
    multi_timeframe_pivots.sort_index(level='date', inplace=True)
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot,
                                               zero_size_allowed=after_under_process_date(date_range_str))
    return multi_timeframe_pivots
    # _multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    # _multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)
    # multi_timeframe_pivots = pd.DataFrame()
    # for timeframe in config.structure_timeframes[::-1][2:]:
    #     _pivots = single_timeframe(_multi_timeframe_peaks_n_valleys, anti_trigger_timeframe(timeframe))
    #     timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)
    #     trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, trigger_timeframe(timeframe))
    #     _pivots = pivots_level_n_margins(_pivots, _pivots, timeframe, timeframe_ohlcva, trigger_timeframe_ohlcva)
    #     _pivots['activation_time'] = _pivots.index
    #     _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
    #     _pivots['hit'] = 0  # update_hits(multi_timeframe_pivots)
    #     _pivots['is_overlap_of'] = None
    #     _pivots['deactivated_at'] = None
    #     _pivots['archived_at'] = None
    #
    #     if len(_pivots) > 0:
    #         # _pivots = cast_and_validate(_pivots, Pivot)
    #         _pivots['timeframe'] = anti_pattern_timeframe(timeframe)
    #         _pivots.set_index('timeframe', append=True, inplace=True)
    #         _pivots = _pivots.swaplevel()
    #         multi_timeframe_pivots = pd.concat([multi_timeframe_pivots, _pivots])
    # multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot)
    # return multi_timeframe_pivots


def read_multi_timeframe_top_pivots(date_range_str: str = None):
    result = read_file(date_range_str, 'multi_timeframe_top_pivots',
                       generate_multi_timeframe_top_pivots, MultiTimeframePivot)
    return result


@measure_time
def generate_multi_timeframe_top_pivots(date_range_str: str = None, file_path: str = config.path_of_data):
    # tops of timeframe which the timeframe C
    # is its pattern timeframe
    if date_range_str is None:
        date_range_str = config.processing_date_range
    _tops_pivots = tops_pivots(date_range_str)
    # plot_multi_timeframe_pivots(_tops_pivots, name='multi_timeframe_top_pivots')
    _tops_pivots.sort_index(inplace=True, level='date')
    _tops_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_top_pivots.{date_range_str}.zip'),
        compression='zip')
    MT.extract_to_data_path(os.path.join(file_path, f'multi_timeframe_top_pivots.{date_range_str}.zip'))
