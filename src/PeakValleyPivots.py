import os
from typing import Literal

import pandas as pd
from pandera import typing as pt

from Config import config, TopTYPE
from MetaTrader import MT
from PanderaDFM.Pivot import MultiTimeframePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, peaks_only, valleys_only
from PivotsHelper import pivots_level_n_margins, level_ttl
from atr import read_multi_timeframe_ohlcva
from helper.data_preparation import single_timeframe, anti_trigger_timeframe, cast_and_validate, \
    read_file, after_under_process_date, empty_df, concat
from helper.helper import measure_time


def atr_top_pivots(date_range_str: str = None, structure_timeframe_shortlist: List['str'] = None) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
    for peaks:
        (
            (find first candle before far >= 3*ATR:
                if there is not a peak between these 2)
                and
            (find first candle after far >= 1*ATR:
                if there is not a peak between these 2)
        ):
            the peak is a pivot
    :param date_range_str:
    :param structure_timeframe_shortlist:
    :return:
    """

    """
    for timeframe:
        peaks/valleys with any :
            -valley/peak after it's previous peaks/valleys which ist's high/low is
                3 ATR gt/lt peaks/valleys high/low and
    """
    # multi_timeframe_trends = read_multi_timeframe_bull_bear_side_trends(date_range_str)  # todo: test
    mt_tops = read_multi_timeframe_peaks_n_valleys(date_range_str)
    # add previous and next peaks and valleys
    peaks = peaks_only(mt_tops)
    valleys = valleys_only(mt_tops)
    mt_tops = peak_or_valley_add_adjacent_tops(mt_tops, peaks, valleys, top_type)
    # mt_tops['previous_peak_value'] = mt_tops.loc[mt_tops['previous_peak_time'].values(), 'high']
    # merge valleys into tops to extract adjacent valley
    mt_tops = pd.merge_asof(mt_tops, valleys[['adjacent_top']], left_index=True, right_on='forward_index',
                            direction='forward',
                            suffixes=("_x", ''))
    mt_tops['next_valley_time'] = mt_tops['adjacent_top']
    # mt_tops['next_valley_value'] = mt_tops.loc[mt_tops['next_valley_time'].values(), 'low']
    mt_tops = pd.merge_asof(mt_tops, valleys[['adjacent_top']], left_index=True, right_on='backward_index',
                            direction='backward',
                            suffixes=("_x", ''))
    mt_tops['previous_valley_time'] = mt_tops['adjacent_top']
    # mt_tops['previous_valley_value'] = mt_tops.loc[mt_tops['previous_valley_time'].values(), 'low']
    long_trends = multi_timeframe_trends[multi_timeframe_trends['movement'] >= multi_timeframe_trends['atr']]
    # find peaks surrounded with enough far valleys:

    """
        movement_start_value: pt.Series[float] = pandera.Field(nullable=True)
        movement_end_value: pt.Series[float] = pandera.Field(nullable=True)
        movement_start_time: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
        movement_end_time
        """
    """
        if the boundary movement > 3 ATR:
            pivot_return:
                there is no trend ends between this trend's end and first candle after movement_end_time:
                    Bullish: low < movement_end_value - 1 ATR
                    Bearish: high > movement_end_value + 1 ATR
    :param date_range_str:
    :param structure_timeframe_shortlist:
    :return:
    """
    # if there is a trand which ends after the trend, if there is a >= 1 ATR jump between their end, we have a pivot.
    for index, trend in long_trends.iterrows():
    # return_time = first_return_confirmation_candle()

    raise NotImplementedError


def peak_or_valley_add_adjacent_tops(mt_tops, tops, top_type: TopTYPE):
    tops['forward_index'] = tops.index.shift(-1, freq=config.timeframe[0])
    tops['backward_index'] = tops.index.shift(1, freq=config.timeframe[0])
    tops['adjacent_top'] = tops.index
    # merge peaks into tops to extract adjacent peaks
    mt_tops = pd.merge_asof(mt_tops, tops[['adjacent_top']], left_index=True, right_on='forward_index',
                            direction='forward',
                            suffixes=("_x", ''))
    mt_tops[f'next_{top_type.value}_time'] = mt_tops['adjacent_top']
    # mt_tops['next_peak_value'] = mt_tops.loc[mt_tops['next_peak_time'].values(), 'high']
    mt_tops = pd.merge_asof(mt_tops, tops[['adjacent_top']], left_index=True, right_on='backward_index',
                            direction='backward',
                            suffixes=("_x", ''))
    mt_tops['previous_{top_type.value}_time'] = mt_tops['adjacent_top']
    return mt_tops


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
        # we use the 1H chart for 1D tops because they are creating 1H classic levels.
        timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)
        trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)  # trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(pivot_peaks_n_valleys=_pivots, timeframe_pivots=_pivots,
                                         timeframe=timeframe,
                                         candle_body_source=timeframe_ohlcva,
                                         internal_atr_source=timeframe_ohlcva,
                                         breakout_atr_source=trigger_timeframe_ohlcva,
                                         )
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0
        _pivots['is_overlap_of'] = None
        _pivots['deactivated_at'] = None
        _pivots['archived_at'] = None
        if len(_pivots) > 0:
            _pivots['timeframe'] = timeframe
            _pivots = _pivots.set_index('timeframe', append=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = concat(multi_timeframe_pivots, _pivots)
    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot,
                                               zero_size_allowed=after_under_process_date(date_range_str))
    return multi_timeframe_pivots


def zz_tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivot]:
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
        # we use the 1H chart for 1D tops because they are creating 1H classic levels.
        timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)
        trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)  # trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(pivot_peaks_n_valleys=_pivots, timeframe_pivots=_pivots,
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
            _pivots['timeframe'] = timeframe
            _pivots = _pivots.set_index('timeframe', append=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = concat(multi_timeframe_pivots, _pivots)
    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot,
                                               zero_size_allowed=after_under_process_date(date_range_str))
    return multi_timeframe_pivots


def read_multi_timeframe_top_pivots(date_range_str: str = None):
    result = read_file(date_range_str, 'multi_timeframe_top_pivots',
                       generate_multi_timeframe_top_pivots, MultiTimeframePivot)
    return result


@measure_time
def generate_multi_timeframe_top_pivots(date_range_str: str = None, file_path: str = None):
    # tops of anti-trigger timeframe
    if date_range_str is None:
        date_range_str = config.processing_date_range
    if file_path is None:
        file_path = config.path_of_data
    _tops_pivots = tops_pivots(date_range_str)
    _tops_pivots = _tops_pivots.sort_index(level='date')
    _tops_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_top_pivots.{date_range_str}.zip'),
        compression='zip')
    MT.extract_to_data_path(os.path.join(file_path, f'multi_timeframe_top_pivots.{date_range_str}.zip'))
