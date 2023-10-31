# import talib as ta
import os

import numpy as np
import pandas as pd
import pandera.typing as pt
from plotly import graph_objects as plgo

from Candle import read_multi_timeframe_ohlcva, expand_date_range
from Config import config, INFINITY_TIME_DELTA, TopTYPE
from DataPreparation import read_file, single_timeframe, df_timedelta_to_str, cast_and_validate
from FigurePlotter.DataPreparation_plotter import plot_ohlcva
from FigurePlotter.plotter import save_figure, file_id, plot_multiple_figures, timeframe_color
from MetaTrader import MT
from Model.MultiTimeframeOHLCV import OHLCV
from Model.MultiTimeframePeakValleys import PeaksValleys, MultiTimeframePeakValleys
from helper import measure_time, date_range


# def calculate_strength(peaks_or_valleys: pd.DataFrame, mode: TopTYPE, ohlcv: pd.DataFrame):
#     # todo: test calculate_strength
#     start_time_of_prices = ohlcv.index[0]
#     end_time_of_prices = ohlcv.index[-1]
#     if 'strength' not in peaks_or_valleys.columns:
#         peaks_or_valleys = peaks_or_valleys.copy()
#         peaks_or_valleys['strength'] = INFINITY_TIME_DELTA
#
#     for i, i_timestamp in enumerate(peaks_or_valleys.index.values):
#         if peaks_or_valleys.index[i] == Timestamp('2017-10-06 00:18:00'):
#             pass
#         if i_timestamp > start_time_of_prices:
#             _left_distance = left_distance(peaks_or_valleys, i, mode, ohlcv)
#             if _left_distance == INFINITY_TIME_DELTA:
#                 _left_distance = peaks_or_valleys.index[i] - start_time_of_prices
#         if i_timestamp < end_time_of_prices:
#             _right_distance = right_distance(peaks_or_valleys, i, mode, ohlcv)
#         if min(_left_distance, _right_distance) < pd.to_timedelta(config.timeframes[0]):
#             raise Exception(
#                 f'Strength expected to be greater than or equal {config.timeframes[0]} but is '
#                 f'min({_left_distance},{_right_distance})={min(_left_distance, _right_distance)} @ "{i_timestamp}"')
#         peaks_or_valleys.loc[i_timestamp, 'strength'] = min(_left_distance, _right_distance)
#     peaks_or_valleys['strength'] = peaks_or_valleys['strength'].dt.total_seconds()
#     return peaks_or_valleys

def calculate_strength(peaks_or_valleys: pt.DataFrame[PeaksValleys], top_type: TopTYPE,
                       ohlcv: pt.DataFrame[OHLCV]):
    # todo: test calculate_strength
    if len(peaks_or_valleys) == 0:
        return peaks_or_valleys
    start_time_of_prices = ohlcv.index[0]
    peaks_or_valleys['strength'] = INFINITY_TIME_DELTA

    if top_type == TopTYPE.PEAK:
        compare_column = 'high'

        def compare_op(x, y):
            return x > y
    else:
        compare_column = 'low'

        def compare_op(x, y):
            return x < y

    # ohlcv_top_indexes = ohlcv.index.isin(peaks_or_valleys.index)
    ohlcv_top_indexes = peaks_or_valleys.index

    # I want to add a column to ohlcv like next_crossing_ohlcv which represent index of nearest ohlcv row which it's
    # high value is greater than high value of the row.

    # Create a boolean mask for values where the 'high' column is greater than its previous value.
    next_higher_mask = compare_op(ohlcv[compare_column].shift(-1), ohlcv[compare_column])
    ohlcv['next_crossing_ohlcv'] = np.where(next_higher_mask, ohlcv.index.shift(-1), np.nan).bfill()

    previous_higher_mask = compare_op(ohlcv[compare_column].shift(1), ohlcv[compare_column])
    ohlcv['previous_crossing_ohlcv'] = np.where(previous_higher_mask, ohlcv.index.shift(1), np.nan).ffill()
    peaks_or_valleys['left_distance'] = peaks_or_valleys.index - ohlcv.loc[ohlcv_top_indexes, 'previous_crossing_ohlcv']
    left_na_indexes = peaks_or_valleys[pd.isna(peaks_or_valleys['left_distance'])].index
    peaks_or_valleys.loc[left_na_indexes, 'left_distance'] = left_na_indexes - start_time_of_prices
    peaks_or_valleys['right_distance'] = ohlcv.loc[ohlcv_top_indexes, 'next_crossing_ohlcv'] - peaks_or_valleys.index
    right_na_indexes = peaks_or_valleys[pd.isna(peaks_or_valleys['right_distance'])].index
    peaks_or_valleys.loc[right_na_indexes, 'right_distance'] = INFINITY_TIME_DELTA
    peaks_or_valleys['strength'] = peaks_or_valleys[['left_distance', 'right_distance']].min(axis=1)

    peaks_or_valleys['strength'] = peaks_or_valleys['strength'].dt.total_seconds()
    return peaks_or_valleys


def mask_of_greater_tops(peaks_valleys: pd.DataFrame, needle: float, mode: TopTYPE):
    if mode == TopTYPE.PEAK:
        return peaks_valleys[peaks_valleys['high'] > needle['high']]
    else:  # mode == TopTYPE.VALLEY
        return peaks_valleys[peaks_valleys['low'] < needle['low']]


def left_distance(peaks_or_valleys: pd.DataFrame, location: int, mode: TopTYPE, ohlcv: pd.DataFrame) \
        -> pt.Timedelta:
    if location == 0:
        return INFINITY_TIME_DELTA
    left_more_significant_tops = mask_of_greater_tops(ohlcv[ohlcv.index < peaks_or_valleys.index[location]],
                                                      peaks_or_valleys.iloc[location],
                                                      mode)
    if len(left_more_significant_tops.index.values) > 0:
        _left_distance = peaks_or_valleys.index[location] - left_more_significant_tops.index[-1]
        if _left_distance <= pd.to_timedelta(config.timeframes[0]):
            raise Exception(
                f'left_distance({_left_distance}) expected to be greater than '
                f'config.timeframes[0]:{config.timeframes[0]} @{location}={peaks_or_valleys.index[location]}')
        return _left_distance
    else:
        return INFINITY_TIME_DELTA


def right_distance(peaks_or_valleys: pd.DataFrame, location: int, mode: TopTYPE, ohlcv: pd.DataFrame) \
        -> pt.Timedelta:
    if location == len(peaks_or_valleys):
        return INFINITY_TIME_DELTA
    right_more_significant_tops = mask_of_greater_tops(ohlcv[ohlcv.index > peaks_or_valleys.index[location]],
                                                       peaks_or_valleys.iloc[location], mode)
    if len(right_more_significant_tops.index.values) > 0:
        _right_distance = right_more_significant_tops.index[0] - peaks_or_valleys.index[location]
        if _right_distance <= pd.to_timedelta(config.timeframes[0]):
            raise Exception(
                f'right_distance({_right_distance}) expected to be greater than '
                f'config.timeframes[0]:{config.timeframes[0]} @{location}={peaks_or_valleys.index[location]}')
        return _right_distance
    else:
        return INFINITY_TIME_DELTA


def map_strength_to_frequency(peaks_valleys: pd.DataFrame) -> pd.DataFrame:
    peaks_valleys.insert(len(peaks_valleys.columns), 'timeframe', None)

    for i in range(len(config.timeframes)):
        for t_peak_valley_index in peaks_valleys[
            peaks_valleys['strength'] > pd.to_timedelta(config.timeframes[i])
        ].index.values:
            peaks_valleys.at[t_peak_valley_index, 'timeframe'] = config.timeframes[i]
    peaks_valleys = peaks_valleys[pd.notna(peaks_valleys['timeframe'])]
    return peaks_valleys


# @measure_time
def plot_peaks_n_valleys(ohlcva: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'ATR']),
                         peaks: pd = pd.DataFrame(columns=['high', 'timeframe']),
                         valleys: pd = pd.DataFrame(columns=['low', 'timeframe']),
                         name: str = '', show: bool = True, save: bool = True) -> plgo.Figure:
    """
        Plot candlesticks with highlighted peaks and valleys.

        Parameters:
            ohlcva (pd.DataFrame): DataFrame containing OHLC data plus ATR.
            peaks (pd.DataFrame): DataFrame containing peaks data.
            valleys (pd.DataFrame): DataFrame containing valleys data.
            name (str): The name of the plot.
            show (bool): Whether to show
            save (bool): Whether to save

        Returns:
            plgo.Figure: The Plotly figure object containing the candlestick plot with peaks and valleys highlighted.
        """
    fig = plot_ohlcva(ohlcva, name=name, save=False, show=False)
    if len(peaks) > 0:
        for timeframe in config.timeframes:
            _indexes, _labels = [], []
            timeframe_peaks = single_timeframe(peaks, timeframe)
            [(_indexes.append(_x), _labels.append(
                f"{timeframe}({df_timedelta_to_str(row['strength'])}@{_x.strftime('%m/%d %H:%M')})={int(row['high'])}"))
             for _x, row in timeframe_peaks.iterrows()]
            fig.add_scatter(x=_indexes, y=timeframe_peaks['high'] + 1, mode="markers", name=f'P{timeframe}',
                            marker=dict(symbol="triangle-up", color=timeframe_color(timeframe)),
                            hovertemplate="%{text}", text=_labels)
    if len(valleys) > 0:
        for timeframe in config.timeframes:
            timeframe_valleys = single_timeframe(valleys, timeframe)
            _indexes, _labels = [], []
            [(_indexes.append(_x), _labels.append(
                f"{timeframe}({df_timedelta_to_str(row['strength'])}@{_x.strftime('%m/%d %H:%M')})={int(row['low'])}"))
             for _x, row in timeframe_valleys.iterrows()]
            fig.add_scatter(x=_indexes, y=timeframe_valleys['low'] - 1, mode="markers", name=f'V{timeframe}',
                            legendgroup=timeframe,
                            marker=dict(symbol="triangle-down", color=timeframe_color(timeframe)),
                            hovertemplate="%{text}", text=_labels)
        fig.update_layout(hovermode='x unified')
    # fig.update_layout(title_text=name)

    if show: fig.show()
    if save:
        save_figure(fig, f'peaks_n_valleys.{file_id(ohlcva, name)}', )
    return fig


def peaks_only(peaks_n_valleys: pd.DataFrame) -> pd.DataFrame:
    """
        Filter peaks from the DataFrame containing peaks and valleys data.

        Parameters:
            peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.

        Returns:
            pd.DataFrame: DataFrame containing only peaks data.
    """
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.PEAK.value]


def valleys_only(peaks_n_valleys: pd.DataFrame) -> pd.DataFrame:
    """
        Filter valleys from the DataFrame containing peaks and valleys data.

        Parameters:
            peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.

        Returns:
            pd.DataFrame: DataFrame containing only valleys data.
    """
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.VALLEY.value]


def merge_tops(peaks: pd.DataFrame, valleys: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([peaks, valleys]).sort_index(level='date')


def find_peaks_n_valleys(base_ohlcv: pd.DataFrame,
                         sort_index: bool = True) -> pd.DataFrame:  # , max_cycles=100):
    mask_of_sequence_of_same_value = (base_ohlcv['high'] == base_ohlcv['high'].shift(1))
    list_to_check = mask_of_sequence_of_same_value.loc[lambda x: x == True]
    list_of_same_high_lows_sequence = base_ohlcv.loc[mask_of_sequence_of_same_value].index
    list_to_check = list_of_same_high_lows_sequence
    # if Timestamp('2017-12-27 06:22:00') in list_to_check:
    #     pass
    # if Timestamp('2017-12-27 06:23:00') in list_to_check:
    #     pass
    # if Timestamp('2017-12-27 06:24:00') in list_to_check:
    #     pass
    # if Timestamp('2017-12-27 06:28:00') in list_to_check:
    #     pass
    # if Timestamp('2017-12-27 06:29:00') in list_to_check:
    #     pass
    # if Timestamp('2017-12-27 06:30:00') in list_to_check:
    #     pass
    none_repeating_ohlcv = base_ohlcv.drop(list_of_same_high_lows_sequence)
    mask_of_peaks = (none_repeating_ohlcv['high'] > none_repeating_ohlcv['high'].shift(1)) & (
            none_repeating_ohlcv['high'] > none_repeating_ohlcv['high'].shift(-1))
    _peaks = none_repeating_ohlcv.loc[mask_of_peaks].copy()
    _peaks['peak_or_valley'] = TopTYPE.PEAK.value

    mask_of_sequence_of_same_value = (base_ohlcv['low'] == base_ohlcv['low'].shift(1))
    list_of_same_high_lows_sequence = base_ohlcv.loc[mask_of_sequence_of_same_value].index
    none_repeating_ohlcv = base_ohlcv.drop(list_of_same_high_lows_sequence)

    mask_of_valleys = (none_repeating_ohlcv['low'] < none_repeating_ohlcv['low'].shift(1)) & (
            none_repeating_ohlcv['low'] < none_repeating_ohlcv['low'].shift(-1))
    _valleys = none_repeating_ohlcv.loc[mask_of_valleys].copy()
    _valleys['peak_or_valley'] = TopTYPE.VALLEY.value
    _peaks_n_valleys: pd.DataFrame = pd.concat([_peaks, _valleys])
    _peaks_n_valleys = _peaks_n_valleys.loc[:, ['open', 'high', 'low', 'close', 'volume', 'peak_or_valley']]
    return _peaks_n_valleys.sort_index(level='date') if sort_index else _peaks_n_valleys


# @measure_time
def major_peaks_n_valleys(multi_timeframe_peaks_n_valleys: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Filter rows from multi_timeframe_peaks_n_valleys with a timeframe equal to or greater than the specified timeframe.

    Parameters:
        multi_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data with 'timeframe' index.
        timeframe (str): The timeframe to filter rows.

    Returns:
        pd.DataFrame: DataFrame containing rows with timeframe equal to or greater than the specified timeframe.
    """
    result = higher_or_eq_timeframe_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
    return result


def higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valleys: pd.DataFrame, timeframe: str):
    try:
        index = config.timeframes.index(timeframe)
    except ValueError as e:
        raise Exception(f'timeframe:{timeframe} should be in [{config.timeframes}]!')
    except Exception as e:
        raise e
    # result = peaks_n_valleys.loc[peaks_n_valleys.index.isin(config.timeframes[index:], level='timeframe')]
    result = peaks_n_valleys.loc[peaks_n_valleys.index.get_level_values('timeframe').isin(config.timeframes[index:])]
    return result


# @measure_time
def plot_multi_timeframe_peaks_n_valleys(multi_timeframe_peaks_n_valleys, date_range_str: str, show=True, save=True,
                                         path_of_plot=config.path_of_plots):
    multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)

    figures = []
    _multi_timeframe_peaks = peaks_only(multi_timeframe_peaks_n_valleys)
    _multi_timeframe_valleys = valleys_only(multi_timeframe_peaks_n_valleys)
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_peaks_n_valleys(single_timeframe(multi_timeframe_ohlcva, timeframe),
                                            peaks=major_peaks_n_valleys(_multi_timeframe_peaks, timeframe),
                                            valleys=major_peaks_n_valleys(_multi_timeframe_valleys, timeframe),
                                            name=f'{timeframe} Peaks n Valleys', show=False, save=False))

    fig = plot_multiple_figures(figures, name=f'multi_timeframe_peaks_n_valleys{file_id(multi_timeframe_ohlcva)}',
                                show=show, save=save,
                                path_of_plot=path_of_plot)
    return fig


def top_timeframe(tops: pt.DataFrame[PeaksValleys]) -> pt.DataFrame[PeaksValleys]:
    """
    _peaks_n_valleys['timeframe'] = [strength_to_timeframe(row['strength']) for index, row in
                                     _peaks_n_valleys.iterrows()]
    :param tops:
    :return:
    """
    tops['timeframe'] = config.timeframes[0]
    for _, timeframe in enumerate(config.timeframes[1:]):
        tops.loc[
            (tops['strength'] >= pd.to_timedelta(timeframe).total_seconds() * 2)
            , 'timeframe'
        ] = timeframe
    return tops


# def zz_strength_to_timeframe(strength: timedelta):
#     if strength < pd.to_timedelta(config.timeframes[0]):
#         raise Exception(f'strength:{strength} expected to be bigger than '
#                         f'config.timeframes[0]:{config.timeframes[0]}/({pd.to_timedelta(config.timeframes[0])})')
#     for i, timeframe in enumerate(config.timeframes):
#         if pd.to_timedelta(timeframe) * 2 >= strength:
#             return config.timeframes[i - 1]
#     return config.timeframes[-1]


def multi_timeframe_peaks_n_valleys(date_range_str) \
        -> pt.DataFrame[MultiTimeframePeakValleys]:
    multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)

    base_ohlcv = single_timeframe(multi_timeframe_ohlcva, config.timeframes[0])

    _peaks_n_valleys = find_peaks_n_valleys(base_ohlcv, sort_index=False)

    _peaks_n_valleys = calculate_strength_of_peaks_n_valleys(base_ohlcv, _peaks_n_valleys)

    # _peaks_n_valleys = _peaks_n_valleys.astype({
    #     'strength': 'timedelta64[s]'
    # })
    _peaks_n_valleys = top_timeframe(_peaks_n_valleys)

    _peaks_n_valleys.set_index('timeframe', append=True, inplace=True)
    _peaks_n_valleys = _peaks_n_valleys.swaplevel()
    _peaks_n_valleys = _peaks_n_valleys.sort_index(level='date')

    cast_and_validate(_peaks_n_valleys, MultiTimeframePeakValleys)
    return _peaks_n_valleys


@measure_time
def generate_multi_timeframe_peaks_n_valleys(date_range_str, file_path: str = config.path_of_data):
    biggest_timeframe = config.timeframes[-1]
    expanded_date_range = expand_date_range(date_range_str,
                                            time_delta=config.ATR_timeperiod * pd.to_timedelta(biggest_timeframe),
                                            mode='both')
    _peaks_n_valleys = multi_timeframe_peaks_n_valleys(expanded_date_range)
    start, end = date_range(date_range_str)
    _peaks_n_valleys = _peaks_n_valleys.loc[
        (start < _peaks_n_valleys.index.get_level_values()) &
        (_peaks_n_valleys.index.get_level_values() < end)]
    # plot_multi_timeframe_peaks_n_valleys(_peaks_n_valleys, date_range_str)
    _peaks_n_valleys.to_csv(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'),
                            compression='zip')
    MT.extract_to_data_path(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'))


@measure_time
def calculate_strength_of_peaks_n_valleys(time_ohlcv, time_peaks_n_valleys):
    peaks = calculate_strength(peaks_only(time_peaks_n_valleys), TopTYPE.PEAK, time_ohlcv)
    valleys = calculate_strength(valleys_only(time_peaks_n_valleys), TopTYPE.VALLEY, time_ohlcv)
    return pd.concat([peaks, valleys]).sort_index(level='date')


def read_multi_timeframe_peaks_n_valleys(date_range_str: str = None) \
        -> pt.DataFrame[PeaksValleys]:
    result = read_file(date_range_str, 'multi_timeframe_peaks_n_valleys',
                       generate_multi_timeframe_peaks_n_valleys,
                       MultiTimeframePeakValleys)
    return result


def old_insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlcv):
    if f'previous_{top_type.value}_index' not in ohlcv.columns:
        ohlcv[f'previous_{top_type.value}_index'] = None
    if f'previous_{top_type.value}_value' not in ohlcv.columns:
        ohlcv[f'previous_{top_type.value}_value'] = None
    if f'next_{top_type.value}_index' not in ohlcv.columns:
        ohlcv[f'next_{top_type.value}_index'] = None
    if f'next_{top_type.value}_value' not in ohlcv.columns:
        ohlcv[f'next_{top_type.value}_value'] = None
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value]
    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
    for i in range(len(tops)):
        if i == len(tops) - 1:
            is_previous_for_indexes = ohlcv.loc[ohlcv.index > tops.index.get_level_values('date')[i]].index
        else:
            is_previous_for_indexes = ohlcv.loc[(ohlcv.index > tops.index.get_level_values('date')[i]) &
                                                (ohlcv.index <= tops.index.get_level_values('date')[i + 1])].index
        if i == 0:
            is_next_for_indexes = ohlcv.loc[ohlcv.index <= tops.index.get_level_values('date')[i]].index
        else:
            is_next_for_indexes = ohlcv.loc[(ohlcv.index > tops.index.get_level_values('date')[i - 1]) &
                                            (ohlcv.index <= tops.index.get_level_values('date')[i])].index
        ohlcv.loc[is_previous_for_indexes, f'previous_{top_type.value}_index'] = tops.index.get_level_values('date')[i]
        ohlcv.loc[is_previous_for_indexes, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        ohlcv.loc[is_next_for_indexes, f'next_{top_type.value}_index'] = tops.index.get_level_values('date')[i]
        ohlcv.loc[is_next_for_indexes, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
    return ohlcv


def insert_previous_n_next_top(top_type, peaks_n_valleys, ohlcv):
    raise Exception('not tested')
    # Define columns
    prev_index_col = f'previous_{top_type.value}_index'
    prev_value_col = f'previous_{top_type.value}_value'
    next_index_col = f'next_{top_type.value}_index'
    next_value_col = f'next_{top_type.value}_value'

    # Ensure columns exist
    for col in [prev_index_col, prev_value_col, next_index_col, next_value_col]:
        if col not in ohlcv.columns:
            ohlcv[col] = None

    # Filter the relevant tops
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value].copy()

    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'

    # Using `shift()` to create the previous and next columns
    tops[prev_index_col] = tops.index.to_series().shift(-1)
    tops[prev_value_col] = tops[high_or_low].shift(-1)
    tops[next_index_col] = tops.index.to_series().shift(1)
    tops[next_value_col] = tops[high_or_low].shift(1)

    # Using `merge_asof` to efficiently merge the previous and next values
    ohlcv = pd.merge_asof(ohlcv.sort_index(), tops[[prev_index_col, prev_value_col]],
                          left_index=True, right_index=True, direction='forward', suffixes=('', '_y'))
    ohlcv = pd.merge_asof(ohlcv.sort_index(), tops[[next_index_col, next_value_col]],
                          left_index=True, right_index=True, direction='backward', suffixes=('', '_y'))

    # Cleaning any duplicate columns
    for col in ohlcv.columns:
        if col.endswith('_y'):
            ohlcv.drop(col, axis=1, inplace=True)

    return ohlcv
