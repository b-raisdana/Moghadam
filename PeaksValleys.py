# import talib as ta
import os
from datetime import timedelta

import pandas as pd
from pandas import Timestamp
from plotly import graph_objects as plgo

from Config import config, INFINITY_TIME_DELTA, TopTYPE
from DataPreparation import read_file, read_multi_timeframe_ohlc, single_timeframe, timedelta_to_str, plot_ohlca, \
    plot_ohlc, file_id, read_multi_timeframe_ohlca
from FigurePlotters import plot_multiple_figures

DEBUG = True


# def zz_generate_peaks_n_valleys(date_range_string: str) -> None:
#     # ohlc = pd.read_csv('ohlc.17-10-06.00-00T17-10-06.23-59.zip', sep=',', header=0, index_col='date',
#     #                               parse_dates=['date'])
#     ohlc = read_ohlc(date_range_string)
#     _peaks, _valleys = zz_find_peaks_n_valleys(ohlc)
#     # _peaks.to_csv(
#     #     f'peaks.{ohlc.index[0].strftime("%y-%m-%d.%H-%M")}T'
#     #     f'{ohlc.index[-1].strftime("%y-%m-%d.%H-%M")}.zip',
#     #     compression='zip')
#     # _valleys.to_csv(
#     #     f'valleys.{ohlc.index[0].strftime("%y-%m-%d.%H-%M")}T'
#     #     f'{ohlc.index[-1].strftime("%y-%m-%d.%H-%M")}.zip',
#     #     compression='zip')
#     peaks_n_valleys = pd.concat([_peaks, _valleys]).sort_index()
#     peaks_n_valleys.to_csv(f'peaks_n_valleys.{date_range_string}.zip', compression='zip')
#     plot_peaks_n_valleys(peaks_n_valleys)


# def zz_find_peaks_n_valleys(prices: pd.DataFrame, min_strength=pd.to_timedelta(config.timeframes[2]), significance=None,
#                             max_cycles=100) -> (pd.DataFrame, pd.DataFrame):
#     _peaks = zz_find_peak_or_valleys(prices, True, min_strength, significance, max_cycles)
#     _valleys = zz_find_peak_or_valleys(prices, False, min_strength, significance, max_cycles)
#     return _peaks, _valleys


# def zz_compare_with_next_and_previous(peaks_mode: TopTYPE, peaks_valleys: pd.DataFrame) -> pd.DataFrame:
#     if peaks_mode:
#         peaks_valleys.insert(len(peaks_valleys.columns), 'next_high', peaks_valleys['high'].shift(-1))
#         peaks_valleys.insert(len(peaks_valleys.columns), 'previous_high', peaks_valleys['high'].shift(1))
#         peaks_valleys = peaks_valleys[(peaks_valleys['previous_high'] < peaks_valleys['high']) &
#                                       (peaks_valleys['high'] >= peaks_valleys['next_high'])]
#         peaks_valleys = peaks_valleys.drop(labels=['next_high', 'previous_high'], axis=1)
#     else:  # valleys_mode
#         peaks_valleys.insert(len(peaks_valleys.columns), 'next_low', peaks_valleys['low'].shift(-1))
#         peaks_valleys.insert(len(peaks_valleys.columns), 'previous_low', peaks_valleys['low'].shift(1))
#         peaks_valleys = peaks_valleys[(peaks_valleys['previous_low'] > peaks_valleys['low']) &
#                                       (peaks_valleys['low'] <= peaks_valleys['next_low'])]
#         peaks_valleys = peaks_valleys.drop(labels=['next_low', 'previous_low'], axis=1)
#     return peaks_valleys


def calculate_strength(peaks_or_valleys: pd.DataFrame, mode: TopTYPE,
                       ohlc_with_next_n_previous_high_lows: pd.DataFrame):
    # todo: test calculate_strength
    start_time_of_prices = ohlc_with_next_n_previous_high_lows.index[0]
    end_time_of_prices = ohlc_with_next_n_previous_high_lows.index[-1]
    if 'strength' not in peaks_or_valleys.columns:
        peaks_or_valleys['strength'] = INFINITY_TIME_DELTA

    for i, i_timestamp in enumerate(peaks_or_valleys.index.values):
        if DEBUG and peaks_or_valleys.index[i] == Timestamp('2017-10-06 00:18:00'):
            pass
        if i_timestamp > start_time_of_prices:
            _left_distance = left_distance(peaks_or_valleys, i, mode, ohlc_with_next_n_previous_high_lows)
            if _left_distance == INFINITY_TIME_DELTA:
                _left_distance = peaks_or_valleys.index[i] - start_time_of_prices
        if i_timestamp < end_time_of_prices:
            _right_distance = right_distance(peaks_or_valleys, i, mode, ohlc_with_next_n_previous_high_lows)
        if min(_left_distance, _right_distance) <= pd.to_timedelta(config.timeframes[0]):
            raise Exception(
                f'Strength expected to be greater than config.timeframes[0]({config.timeframes[0]}) which is '
                f'min(_left_distance, _right_distance)=min({_left_distance},{_right_distance})'
                f'={min(_left_distance, _right_distance)} @ "{i_timestamp}"')
        peaks_or_valleys.loc[i_timestamp, 'strength'] = min(_left_distance, _right_distance)
    # output = pd.concat([peaks_or_valleys, reserved_peaks_or_valleys]).sort_index()
    return peaks_or_valleys


def mask_of_greater_tops(peaks_valleys: pd.DataFrame, needle: float, mode: TopTYPE):
    if mode == TopTYPE.PEAK:
        return peaks_valleys[peaks_valleys['high'] > needle['high']]
    else:  # mode == TopTYPE.VALLEY
        return peaks_valleys[peaks_valleys['low'] < needle['low']]


# def left_valley_distance(prices: pd.DataFrame, peaks_valleys: pd.DataFrame, i: int,
#                          left_distance: pd.Timedelta) -> pd.Timedelta:
#     left_lower_valleys = prices[(prices.index < peaks_valleys.index[i]) &
#                                 (prices['low'] < peaks_valleys.iloc[i]['low'])]
#     if len(left_lower_valleys.index.values) > 0:
#         left_distance = (peaks_valleys.index[i] - left_lower_valleys.index[-1])
#         # check if at least one higher valley exist in the range
#         higher_candles_after_left_nearest_lower_valley = \
#             prices[((peaks_valleys.index[i] - left_distance) < prices.index) &
#                    (prices.index < peaks_valleys.index[i]) &
#                    (prices['low'] > peaks_valleys.iloc[i]['low'])]
#         if len(higher_candles_after_left_nearest_lower_valley) == 0:
#             left_distance = timedelta(0)
#     return left_distance
def left_distance(peaks_or_valleys: pd.DataFrame, location: int, mode: TopTYPE, ohlc: pd.DataFrame) -> pd.Timedelta:
    if location == 0:
        return INFINITY_TIME_DELTA
    left_more_significant_tops = mask_of_greater_tops(ohlc[ohlc.index < peaks_or_valleys.index[location]],
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


# def right_valley_distance(prices: pd.DataFrame, peaks_valleys: pd.DataFrame, i: int,
#                           right_distance: pd.Timedelta) -> pd.Timedelta:
#     right_lower_valleys = prices[(prices.index > peaks_valleys.index[i]) &
#                                  (prices['low'] < peaks_valleys.iloc[i]['low'])]
#     if len(right_lower_valleys.index.values) > 0:
#         right_distance = (right_lower_valleys.index[0] - peaks_valleys.index[i])
#         higher_candles_before_right_nearest_lower_valley = \
#             prices[(peaks_valleys.index[i] < prices.index) &
#                    (prices.index < (peaks_valleys.index[i] + right_distance)) &
#                    (prices['low'] > peaks_valleys.iloc[i]['low'])]
#         if len(higher_candles_before_right_nearest_lower_valley) == 0:
#             right_distance = timedelta(0)
#     return right_distance
def right_distance(peaks_or_valleys: pd.DataFrame, location: int, mode: TopTYPE, ohlc: pd.DataFrame) -> pd.Timedelta:
    if location == len(peaks_or_valleys):
        return INFINITY_TIME_DELTA
    right_more_significant_tops = mask_of_greater_tops(ohlc[ohlc.index > peaks_or_valleys.index[location]],
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


# def right_peak_distance(i: int, right_distance: pd.Timedelta, peaks_valleys: pd.DataFrame,
#                         prices: pd.DataFrame) -> pd.Timedelta:
#     right_higher_valleys = prices[(prices.index > peaks_valleys.index[i]) &
#                                   (prices['high'] > peaks_valleys.iloc[i]['high'])]
#     if len(right_higher_valleys.index.values) > 0:
#         right_distance = (right_higher_valleys.index[0] - peaks_valleys.index[i])
#         # lower_candles_before_right_nearest_higher_peak = \
#         #     prices[(peaks_valleys.index[i] < prices.index) &
#         #            (prices.index < (peaks_valleys.index[i] + right_distance)) &
#         #            (prices['high'] < peaks_valleys.iloc[i]['high'])]
#         # if len(lower_candles_before_right_nearest_higher_peak) == 0:
#         #     left_distance = timedelta(0)
#     return right_distance
#
#
# def left_peak_distance(i: int, left_distance: pd.Timedelta, peaks_valleys: pd.DataFrame,
#                        prices: pd.DataFrame) -> pd.Timedelta:
#     left_higher_peaks = prices[(prices.index < peaks_valleys.index[i]) &
#                                (prices['high'] > peaks_valleys.iloc[i]['high'])]
#     if len(left_higher_peaks.index.values) > 0:
#         left_distance = (peaks_valleys.index[i] - left_higher_peaks.index[-1])  #
#         lower_candles_after_left_nearest_higher_peak = \
#             prices[((peaks_valleys.index[i] - left_distance) < prices.index) &
#                    (prices.index < peaks_valleys.index[i]) &
#                    (prices['high'] < peaks_valleys.iloc[i]['high'])]
#         if len(lower_candles_after_left_nearest_higher_peak) == 0:
#             left_distance = timedelta(0)
#     return left_distance


def map_strength_to_frequency(peaks_valleys: pd.DataFrame) -> pd.DataFrame:
    peaks_valleys.insert(len(peaks_valleys.columns), 'timeframe', None)

    for i in range(len(config.timeframes)):
        for t_peak_valley_index in peaks_valleys[
            peaks_valleys['strength'] > pd.to_timedelta(config.timeframes[i])
        ].index.values:
            peaks_valleys.at[t_peak_valley_index, 'timeframe'] = config.timeframes[i]
    peaks_valleys = peaks_valleys[pd.notna(peaks_valleys['timeframe'])]
    return peaks_valleys


# def zz_find_peak_or_valleys(prices: pd.DataFrame, peaks_mode: bool = True, min_strength: timedelta = None,
#                             ignore_n_percent_lowest_strength=None) -> pd.DataFrame:  # , max_cycles=100):
#     valleys_mode = not peaks_mode
#     peaks_valleys: pd = prices.copy()  # pd.DataFrame(prices.index)
#     peaks_valleys.insert(len(peaks_valleys.columns), 'strength', INFINITY_TIME_DELTA)
#     peaks_valleys = zz_compare_with_next_and_previous(peaks_mode, peaks_valleys)
#     peaks_valleys = peaks_valleys[peaks_valleys['volume'] > 0]
#     peaks_valleys = calculate_strength(peaks_valleys, valleys_mode, prices)
#
#     if min_strength is not None:
#         raise Exception('Not tested')
#         peaks_valleys = peaks_valleys['strength' >= min_strength]
#     if ignore_n_percent_lowest_strength is not None:
#         raise Exception('Not implemented')
#         zztodo: extract distribution of strength and ignore n_percent lowest peak_valleys
#         # peak_valley_weights = peaks_valleys['strength'].unique().sort(reverse=True)
#         # if len(peak_valley_weights) > ignore_n_percent_lowest_strength:
#         #     peaks_valleys = peaks_valleys['strength' >= peak_valley_weights[ignore_n_percent_lowest_strength - 1]]
#     peaks_valleys = peaks_valleys[peaks_valleys['strength'] > timedelta(0)]
#
#     peaks_valleys = map_strength_to_frequency(peaks_valleys)
#     return peaks_valleys


def plot_peaks_n_valleys(ohlca: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'ATR']),
                         peaks: pd = pd.DataFrame(columns=['high', 'timeframe']),
                         valleys: pd = pd.DataFrame(columns=['low', 'timeframe']),
                         file_name: str = '', show: bool = True, save: bool = True) -> plgo.Figure:
    """
        Plot candlesticks with highlighted peaks and valleys.

        Parameters:
            ohlca (pd.DataFrame): DataFrame containing OHLC data plus ATR.
            peaks (pd.DataFrame): DataFrame containing peaks data.
            valleys (pd.DataFrame): DataFrame containing valleys data.
            file_name (str): The name of the plot.
            show (bool): Whether to show
            save (bool): Whether to save

        Returns:
            plgo.Figure: The Plotly figure object containing the candlestick plot with peaks and valleys highlighted.
        """
    fig = plot_ohlca(ohlca, name=file_name, save=False, show=False)
    if len(peaks) > 0:
        _indexes, _labels = [], []
        [(_indexes.append(_x[1]), _labels.append(
            f"{_x[0]}/{timedelta_to_str(row['strength'])}@{_x[1].strftime('%m-%d|%H:%M')}={row['high']}"))
         for _x, row in peaks.iterrows()]
        fig.add_scatter(x=_indexes, y=peaks['high'] + 1, mode="markers", name='P',
                        marker=dict(symbol="triangle-up", color="blue"), hovertemplate="%{text}", text=_labels)
    if len(valleys) > 0:
        _indexes, _labels = [], []
        [(_indexes.append(_x[1]), _labels.append(
            f"{_x[0]}({timedelta_to_str(row['strength'])})@{_x[1].strftime('%m-%d|%H:%M')}={row['low']}"))
         for _x, row in valleys.iterrows()]
        fig.add_scatter(x=_indexes, y=valleys['low'] - 1, mode="markers", name='V',
                        marker=dict(symbol="triangle-down", color="blue"), hovertemplate="%{text}", text=_labels)
        fig.update_layout(hovermode='x unified')
    if show: fig.show()
    if save:
        figure_as_html = fig.to_html()
        file_name = os.path.join(config.path_of_plots, f'peaks_n_valleys.{file_id(ohlca, file_name)}.html')
        with open(file_name, '+w') as f:
            f.write(figure_as_html)
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
    return pd.concat([peaks, valleys]).sort_index()


# def zz_read_peaks_n_valleys(date_range_string: str) -> pd.DataFrame:
#     # if not os.path.isfile(f'peaks_n_valleys.{date_range_string}.zip'):
#     #     ohlc = pd.read_csv(f'ohlc.{date_range_string}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])
#     # raise Exception('Not completed!')
#     read_file(date_range_string, 'peaks_n_valleys', zz_generate_peaks_n_valleys)


# def check_multi_timeframe_peaks_n_valleys_columns(multi_timeframe_peaks_n_valleys: pd.DataFrame,
#                                                   raise_exception=False) -> bool:
#     return check_dataframe(multi_timeframe_peaks_n_valleys, config.multi_timeframe_peaks_n_valleys_columns,
#                            raise_exception)


# def find_single_timeframe_peaks_n_valleys(ohlc: pd.DataFrame,
#                                           sort_index: bool = True) -> pd.DataFrame:  # , max_cycles=100):
#     ohlc['next_high'] = ohlc['high'].shift(-1)
#     ohlc['previous_high'] = ohlc['high'].shift(1)
#     ohlc['next_low'] = ohlc['low'].shift(-1)
#     ohlc['previous_low'] = ohlc['low'].shift(1)
#     _peaks = ohlc.loc[(ohlc['high'] > ohlc['previous_high']) & (ohlc['high'] >= ohlc['next_high'])].copy()
#     _peaks['peak_or_valley'] = TopTYPE.PEAK.value
#     _valleys = ohlc.loc[(ohlc['low'] < ohlc['previous_low']) & (ohlc['low'] >= ohlc['next_low'])].copy()
#     _valleys['peak_or_valley'] = TopTYPE.VALLEY.value
#     _peaks_n_valleys = pd.concat([_peaks, _valleys])
#     _peaks_n_valleys = _peaks_n_valleys.loc[:, ['open', 'high', 'low', 'close', 'volume', 'peak_or_valley']]
#     return _peaks_n_valleys.sort_index() if sort_index else _peaks_n_valleys


def find_single_timeframe_peaks_n_valleys(ohlc: pd.DataFrame,
                                          sort_index: bool = True) -> pd.DataFrame:  # , max_cycles=100):
    mask_of_sequence_of_same_value = (ohlc['high'] == ohlc['high'].shift(1))
    sequence_of_same_high_lows = ohlc.loc[mask_of_sequence_of_same_value].index
    none_repeating_ohlc = ohlc.drop(sequence_of_same_high_lows)

    mask_of_peaks = (none_repeating_ohlc['high'] > none_repeating_ohlc['high'].shift(1)) & (
            none_repeating_ohlc['high'] > none_repeating_ohlc['high'].shift(-1))
    _peaks = none_repeating_ohlc.loc[mask_of_peaks]
    _peaks['peak_or_valley'] = TopTYPE.PEAK.value

    mask_of_sequence_of_same_value = (ohlc['low'] == ohlc['low'].shift(1))
    sequence_of_same_high_lows = ohlc.loc[mask_of_sequence_of_same_value].index
    none_repeating_ohlc = ohlc.drop(sequence_of_same_high_lows)

    mask_of_valleys = (none_repeating_ohlc['low'] < none_repeating_ohlc['low'].shift(1)) & (
            none_repeating_ohlc['low'] < none_repeating_ohlc['low'].shift(-1))
    _valleys = none_repeating_ohlc.loc[mask_of_valleys]
    _valleys['peak_or_valley'] = TopTYPE.VALLEY.value

    _peaks_n_valleys = pd.concat([_peaks, _valleys])
    _peaks_n_valleys = _peaks_n_valleys.loc[:, ['open', 'high', 'low', 'close', 'volume', 'peak_or_valley']]
    return _peaks_n_valleys.sort_index() if sort_index else _peaks_n_valleys


def major_peaks_n_valleys(multi_timeframe_peaks_n_valleys: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Filter rows from multi_timeframe_peaks_n_valleys with a timeframe equal to or greater than the specified timeframe.

    Parameters:
        multi_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data with 'timeframe' index.
        timeframe (str): The timeframe to filter rows.

    Returns:
        pd.DataFrame: DataFrame containing rows with timeframe equal to or greater than the specified timeframe.
    """
    return higher_or_eq_timeframe_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
    # timeframe_index = config.timeframes.index(timeframe)
    # relevant_timeframes = config.timeframes[timeframe_index:]
    # return multi_timeframe_peaks_n_valleys[
    #     multi_timeframe_peaks_n_valleys.index.get_level_values('timeframe').isin(relevant_timeframes)]


def higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valleys: pd.DataFrame, timeframe: str):
    try:
        index = config.timeframes.index(timeframe)
    except ValueError as e:
        raise Exception(f'timeframe:{timeframe} should be in [{config.timeframes}]!')
    return peaks_n_valleys.loc[peaks_n_valleys.index.isin(config.timeframes[index:], level='timeframe')]


def plot_multi_timeframe_peaks_n_valleys(multi_timeframe_peaks_n_valleys, multi_timeframe_ohlca, show=True, save=True,
                                         path_of_plot=config.path_of_plots):
    # todo: test plot_multi_timeframe_peaks_n_valleys
    figures = []
    _multi_timeframe_peaks = peaks_only(multi_timeframe_peaks_n_valleys)
    _multi_timeframe_valleys = valleys_only(multi_timeframe_peaks_n_valleys)
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_peaks_n_valleys(single_timeframe(multi_timeframe_ohlca, timeframe),
                                            peaks=major_peaks_n_valleys(_multi_timeframe_peaks, timeframe),
                                            valleys=major_peaks_n_valleys(_multi_timeframe_valleys, timeframe),
                                            file_name=f'{timeframe} Peaks n Valleys', show=False, save=False))

    fig = plot_multiple_figures(figures, file_name='multi_timeframe_peaks_n_valleys', show=show, save=save,
                                path_of_plot=path_of_plot)
    return fig


# def generate_multi_timeframe_peaks_n_valleys(date_range_str, file_path: str = config.path_of_data):
#     multi_timeframe_ohlc = read_multi_timeframe_ohlc()
#     _peaks_n_valleys = pd.DataFrame()
#     for _, timeframe in enumerate(config.timeframes):
#         time_ohlc = single_timeframe(multi_timeframe_ohlc, timeframe)
#         time_peaks_n_valleys = find_single_timeframe_peaks_n_valleys(time_ohlc, sort_index=False)
#         time_peaks_n_valleys = calculate_strength_of_peaks_n_valleys(time_ohlc, time_peaks_n_valleys)
#         time_peaks_n_valleys['timeframe'] = timeframe
#         time_peaks_n_valleys.set_index('timeframe', append=True, inplace=True)
#         time_peaks_n_valleys = time_peaks_n_valleys.swaplevel()
#         _peaks_n_valleys = pd.concat([_peaks_n_valleys, time_peaks_n_valleys])
#     _peaks_n_valleys = _peaks_n_valleys.sort_index()
#     plot_multi_timeframe_peaks_n_valleys(_peaks_n_valleys, multi_timeframe_ohlc)
#     _peaks_n_valleys.to_csv(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'),
#                             compression='zip')
def strength_to_timeframe(strength: timedelta):
    if strength < pd.to_timedelta(config.timeframes[0]):
        raise Exception(f'strength:{strength} expected to be bigger than '
                        f'config.timeframes[0]:{config.timeframes[0]}/({pd.to_timedelta(config.timeframes[0])})')
    for i, timeframe in enumerate(config.timeframes):
        if pd.to_timedelta(timeframe) > strength:
            return config.timeframes[i - 1]


def generate_multi_timeframe_peaks_n_valleys(date_range_str, file_path: str = config.path_of_data):
    multi_timeframe_ohlca = read_multi_timeframe_ohlca()
    _peaks_n_valleys = pd.DataFrame()
    # for _, timeframe in enumerate(config.timeframes):
    base_ohlc = single_timeframe(multi_timeframe_ohlca, config.timeframes[0])
    time_peaks_n_valleys = find_single_timeframe_peaks_n_valleys(base_ohlc, sort_index=False)
    time_peaks_n_valleys = calculate_strength_of_peaks_n_valleys(base_ohlc, time_peaks_n_valleys)
    time_peaks_n_valleys['timeframe'] = [strength_to_timeframe(row['strength']) for index, row in
                                         time_peaks_n_valleys.iterrows()]
    time_peaks_n_valleys.set_index('timeframe', append=True, inplace=True)
    time_peaks_n_valleys = time_peaks_n_valleys.swaplevel()
    _peaks_n_valleys = pd.concat([_peaks_n_valleys, time_peaks_n_valleys])
    _peaks_n_valleys = _peaks_n_valleys.sort_index()
    plot_multi_timeframe_peaks_n_valleys(_peaks_n_valleys, multi_timeframe_ohlca)
    _peaks_n_valleys.to_csv(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'),
                            compression='zip')
    # return _peaks_n_valleys


def calculate_strength_of_peaks_n_valleys(time_ohlc, time_peaks_n_valleys):
    peaks = calculate_strength(peaks_only(time_peaks_n_valleys), TopTYPE.PEAK, time_ohlc)
    valleys = calculate_strength(valleys_only(time_peaks_n_valleys), TopTYPE.VALLEY, time_ohlc)
    return pd.concat([peaks, valleys]).sort_index()


def read_multi_timeframe_peaks_n_valleys(date_range_str: str = config.under_process_date_range):
    return read_file(date_range_str, 'multi_timeframe_peaks_n_valleys', generate_multi_timeframe_peaks_n_valleys)
