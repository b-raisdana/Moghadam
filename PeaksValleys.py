# import talib as ta
from datetime import timedelta

import pandas as pd
from pandas import Timestamp
from plotly import graph_objects as plgo

from Config import config, INFINITY_TIME_DELTA, TopTYPE
from DataPreparation import read_file, check_dataframe, read_multi_timeframe_ohlc, read_ohlc
from FigurePlotters import plotfig

DEBUG = True


def generate_peaks_n_valleys(date_range_string: str) -> None:
    # ohlc = pd.read_csv('ohlc.17-10-06.00-00T17-10-06.23-59.zip', sep=',', header=0, index_col='date',
    #                               parse_dates=['date'])
    ohlc = read_ohlc(date_range_string)
    _peaks, _valleys = find_peaks_n_valleys(ohlc)
    # _peaks.to_csv(
    #     f'peaks.{ohlc.index[0].strftime("%y-%m-%d.%H-%M")}T'
    #     f'{ohlc.index[-1].strftime("%y-%m-%d.%H-%M")}.zip',
    #     compression='zip')
    # _valleys.to_csv(
    #     f'valleys.{ohlc.index[0].strftime("%y-%m-%d.%H-%M")}T'
    #     f'{ohlc.index[-1].strftime("%y-%m-%d.%H-%M")}.zip',
    #     compression='zip')
    peaks_n_valleys = pd.concat([_peaks, _valleys]).sort_index()
    peaks_n_valleys.to_csv(f'peaks_n_valleys.{date_range_string}.zip', compression='zip')
    plot_peaks_n_valleys(peaks_n_valleys)


def find_peaks_n_valleys(prices: pd.DataFrame, min_strength=pd.to_timedelta(config.timeframes[2]), significance=None,
                         max_cycles=100) -> (pd.DataFrame, pd.DataFrame):
    _peaks = find_peak_or_valleys(prices, True, min_strength, significance, max_cycles)
    _valleys = find_peak_or_valleys(prices, False, min_strength, significance, max_cycles)
    return _peaks, _valleys


def compare_with_next_and_previous(peaks_mode: TopTYPE, peaks_valleys: pd.DataFrame) -> pd.DataFrame:
    if peaks_mode:
        peaks_valleys.insert(len(peaks_valleys.columns), 'next_high', peaks_valleys['high'].shift(-1))
        peaks_valleys.insert(len(peaks_valleys.columns), 'previous_high', peaks_valleys['high'].shift(1))
        peaks_valleys = peaks_valleys[(peaks_valleys['previous_high'] < peaks_valleys['high']) &
                                      (peaks_valleys['high'] >= peaks_valleys['next_high'])]
        peaks_valleys = peaks_valleys.drop(labels=['next_high', 'previous_high'], axis=1)
    else:  # valleys_mode
        peaks_valleys.insert(len(peaks_valleys.columns), 'next_low', peaks_valleys['low'].shift(-1))
        peaks_valleys.insert(len(peaks_valleys.columns), 'previous_low', peaks_valleys['low'].shift(1))
        peaks_valleys = peaks_valleys[(peaks_valleys['previous_low'] > peaks_valleys['low']) &
                                      (peaks_valleys['low'] <= peaks_valleys['next_low'])]
        peaks_valleys = peaks_valleys.drop(labels=['next_low', 'previous_low'], axis=1)
    return peaks_valleys


def calculate_strength(peaks_valleys: pd.DataFrame, valleys_mode: bool, prices: pd.DataFrame):
    for i in range(len(peaks_valleys.index.values)):
        if DEBUG and peaks_valleys.index[i] == Timestamp('2017-01-04 11:17:00'):
            pass
        left_distance = INFINITY_TIME_DELTA
        right_distance = INFINITY_TIME_DELTA

        if valleys_mode:
            if peaks_valleys.index[i] > prices.index[0]:
                left_distance = left_valley_distance(prices, peaks_valleys, i, left_distance)
            if peaks_valleys.index[i] < prices.index[-1]:
                right_distance = right_valley_distance(prices, peaks_valleys, i, right_distance)
        else:  # peaks_mode
            if peaks_valleys.index[i] > prices.index[0]:
                left_distance = left_peak_distance(i, left_distance, peaks_valleys, prices)
            if peaks_valleys.index[i] < prices.index[-1]:
                right_distance = right_peak_distance(i, right_distance, peaks_valleys, prices)
        if prices.index[0] < peaks_valleys.index[i] < prices.index[-1] and left_distance == INFINITY_TIME_DELTA:
            peaks_valleys.at[peaks_valleys.index[i], 'strength'] \
                = min(peaks_valleys.index[i] - prices.index[0], right_distance,
                      peaks_valleys.iloc[i]['strength'])  # min(i, len(prices) - i)
            continue
        peaks_valleys.at[peaks_valleys.index[i], 'strength'] = \
            min(left_distance, right_distance, peaks_valleys.iloc[i]['strength'])
    return peaks_valleys


def left_valley_distance(prices: pd.DataFrame, peaks_valleys: pd.DataFrame, i: int,
                         left_distance: pd.Timedelta) -> pd.Timedelta:
    left_lower_valleys = prices[(prices.index < peaks_valleys.index[i]) &
                                (prices['low'] < peaks_valleys.iloc[i]['low'])]
    if len(left_lower_valleys.index.values) > 0:
        left_distance = (peaks_valleys.index[i] - left_lower_valleys.index[-1])
        # check if at least one higher valley exist in the range
        higher_candles_after_left_nearest_lower_valley = \
            prices[((peaks_valleys.index[i] - left_distance) < prices.index) &
                   (prices.index < peaks_valleys.index[i]) &
                   (prices['low'] > peaks_valleys.iloc[i]['low'])]
        if len(higher_candles_after_left_nearest_lower_valley) == 0:
            left_distance = timedelta(0)
    return left_distance


def right_valley_distance(prices: pd.DataFrame, peaks_valleys: pd.DataFrame, i: int,
                          right_distance: pd.Timedelta) -> pd.Timedelta:
    right_lower_valleys = prices[(prices.index > peaks_valleys.index[i]) &
                                 (prices['low'] < peaks_valleys.iloc[i]['low'])]
    if len(right_lower_valleys.index.values) > 0:
        right_distance = (right_lower_valleys.index[0] - peaks_valleys.index[i])
        higher_candles_before_right_nearest_lower_valley = \
            prices[(peaks_valleys.index[i] < prices.index) &
                   (prices.index < (peaks_valleys.index[i] + right_distance)) &
                   (prices['low'] > peaks_valleys.iloc[i]['low'])]
        if len(higher_candles_before_right_nearest_lower_valley) == 0:
            right_distance = timedelta(0)
    return right_distance


def right_peak_distance(i: int, right_distance: pd.Timedelta, peaks_valleys: pd.DataFrame,
                        prices: pd.DataFrame) -> pd.Timedelta:
    right_higher_valleys = prices[(prices.index > peaks_valleys.index[i]) &
                                  (prices['high'] > peaks_valleys.iloc[i]['high'])]
    if len(right_higher_valleys.index.values) > 0:
        right_distance = (right_higher_valleys.index[0] - peaks_valleys.index[i])
        # lower_candles_before_right_nearest_higher_peak = \
        #     prices[(peaks_valleys.index[i] < prices.index) &
        #            (prices.index < (peaks_valleys.index[i] + right_distance)) &
        #            (prices['high'] < peaks_valleys.iloc[i]['high'])]
        # if len(lower_candles_before_right_nearest_higher_peak) == 0:
        #     left_distance = timedelta(0)
    return right_distance


def left_peak_distance(i: int, left_distance: pd.Timedelta, peaks_valleys: pd.DataFrame,
                       prices: pd.DataFrame) -> pd.Timedelta:
    left_higher_peaks = prices[(prices.index < peaks_valleys.index[i]) &
                               (prices['high'] > peaks_valleys.iloc[i]['high'])]
    if len(left_higher_peaks.index.values) > 0:
        left_distance = (peaks_valleys.index[i] - left_higher_peaks.index[-1])  #
        lower_candles_after_left_nearest_higher_peak = \
            prices[((peaks_valleys.index[i] - left_distance) < prices.index) &
                   (prices.index < peaks_valleys.index[i]) &
                   (prices['high'] < peaks_valleys.iloc[i]['high'])]
        if len(lower_candles_after_left_nearest_higher_peak) == 0:
            left_distance = timedelta(0)
    return left_distance


def map_strength_to_frequency(peaks_valleys: pd.DataFrame) -> pd.DataFrame:
    peaks_valleys.insert(len(peaks_valleys.columns), 'timeframe', None)

    for i in range(len(config.timeframes)):
        for t_peak_valley_index in peaks_valleys[
            peaks_valleys['strength'] > pd.to_timedelta(config.timeframes[i])
        ].index.values:
            peaks_valleys.at[t_peak_valley_index, 'timeframe'] = config.timeframes[i]
    peaks_valleys = peaks_valleys[pd.notna(peaks_valleys['timeframe'])]
    return peaks_valleys


def find_peak_or_valleys(prices: pd.DataFrame, peaks_mode: bool = True, min_strength: timedelta = None,
                         ignore_n_percent_lowest_strength=None) -> pd.DataFrame:  # , max_cycles=100):
    valleys_mode = not peaks_mode
    peaks_valleys: pd = prices.copy()  # pd.DataFrame(prices.index)
    peaks_valleys.insert(len(peaks_valleys.columns), 'strength', INFINITY_TIME_DELTA)
    peaks_valleys = compare_with_next_and_previous(peaks_mode, peaks_valleys)
    peaks_valleys = peaks_valleys[peaks_valleys['volume'] > 0]
    peaks_valleys = calculate_strength(peaks_valleys, valleys_mode, prices)

    if min_strength is not None:
        raise Exception('Not tested')
        peaks_valleys = peaks_valleys['strength' >= min_strength]
    if ignore_n_percent_lowest_strength is not None:
        raise Exception('Not implemented')
        # todo: extract distribution of strength and ignore n_percent lowest peak_valleys
        # peak_valley_weights = peaks_valleys['strength'].unique().sort(reverse=True)
        # if len(peak_valley_weights) > ignore_n_percent_lowest_strength:
        #     peaks_valleys = peaks_valleys['strength' >= peak_valley_weights[ignore_n_percent_lowest_strength - 1]]
    peaks_valleys = peaks_valleys[peaks_valleys['strength'] > timedelta(0)]

    peaks_valleys = map_strength_to_frequency(peaks_valleys)
    return peaks_valleys


def plot_peaks_n_valleys(ohlc: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
                         peaks: pd = pd.DataFrame(columns=['high', 'timeframe']),
                         valleys: pd = pd.DataFrame(columns=['low', 'timeframe']),
                         name: str = '', do_not_show: bool = False) -> plgo.Figure:
    fig = plotfig(ohlc, name=name, save=False, do_not_show=True)
    if len(peaks) > 0:
        fig.add_scatter(x=peaks.index.values, y=peaks['high'] + 1, mode="markers", name='P',
                        marker=dict(symbol="triangle-up", color="blue"),
                        hovertemplate="%{text}",
                        text=[
                            f"{peaks.loc[_x]['timeframe']}@{peaks.loc[_x]['high']}"
                            for _x in peaks.index.values]
                        )
    if len(valleys) > 0:
        fig.add_scatter(x=valleys.index.values, y=valleys['low'] - 1, mode="markers", name='V',
                        marker=dict(symbol="triangle-down", color="blue"),
                        hovertemplate="%{text}",
                        text=[
                            f"{valleys.loc[_x]['timeframe']}@{valleys.loc[_x]['low']}"
                            for _x in valleys.index.values]
                        )
        fig.update_layout(hovermode='x unified')
        if not do_not_show: fig.show()
    return fig


def peaks_only(peaks_n_valleys: pd.DataFrame) -> pd.DataFrame:
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.PEAK]


def valleys_only(peaks_n_valleys: pd.DataFrame) -> pd.DataFrame:
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.VALLEY]


def higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valleys: pd.DataFrame, timeframe: str):
    try:
        index = config.timeframes.index(timeframe)
    except ValueError as e:
        raise Exception(f'timeframe:{timeframe} should be in [{config.timeframes}]!')
    return peaks_n_valleys.loc[(slice(None), config.timeframes[index:]), :]


def merge_tops(peaks: pd.DataFrame, valleys: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([peaks, valleys]).sort_index()


def read_peaks_n_valleys(date_range_string: str) -> pd.DataFrame:
    # if not os.path.isfile(f'peaks_n_valleys.{date_range_string}.zip'):
    #     ohlc = pd.read_csv(f'ohlc.{date_range_string}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])
    # raise Exception('Not completed!')
    read_file(date_range_string, 'peaks_n_valleys', generate_peaks_n_valleys)


def check_multi_timeframe_peaks_n_valleys_columns(multi_timeframe_peaks_n_valleys: pd.DataFrame,
                                                  raise_exception=False) -> bool:
    return check_dataframe(multi_timeframe_peaks_n_valleys, config.multi_timeframe_peaks_n_valleys_columns,
                           raise_exception)


def find_single_timeframe_peaks_n_valleys(ohlc: pd.DataFrame,
                                          sort_index: bool = True) -> pd.DataFrame:  # , max_cycles=100):
    ohlc['next_high'] = ohlc['high'].shift(-1)
    ohlc['previous_high'] = ohlc['high'].shift(1)
    ohlc['next_low'] = ohlc['low'].shift(-1)
    ohlc['previous_low'] = ohlc['low'].shift(1)
    _peaks = ohlc.loc[(ohlc['high'] > ohlc['previous_high']) & (ohlc['high'] >= ohlc['next_high'])].copy()
    _peaks['peak_or_valley'] = TopTYPE.PEAK
    _valleys = ohlc.loc[(ohlc['low'] < ohlc['previous_low']) & (ohlc['low'] >= ohlc['next_low'])].copy()
    _valleys['peak_or_valley'] = TopTYPE.VALLEY
    _peaks_n_valleys = pd.concat([_peaks, _valleys])
    _peaks_n_valleys = _peaks_n_valleys.loc[:, ['open', 'high', 'low', 'close', 'volume', 'peak_or_valley']]
    return _peaks_n_valleys.sort_index() if sort_index else _peaks_n_valleys


def plot_multi_timeframe_peaks_n_valleys(_peaks_n_valleys):
    # todo: implement   plot_multi_timeframe_peaks_n_valleys
    pass


def generate_multi_timeframe_peaks_n_valleys(date_range_str):
    multi_timeframe_ohlc = read_multi_timeframe_ohlc()
    _peaks_n_valleys = pd.DataFrame()
    for _, timeframe in enumerate(config.timeframes):
        time_peaks_n_valleys = find_single_timeframe_peaks_n_valleys(
            multi_timeframe_ohlc.loc[(slice(None), timeframe), :], sort_index=False)
        # time_peaks_n_valleys['timeframe'] = timeframe
        _peaks_n_valleys = pd.concat([_peaks_n_valleys, time_peaks_n_valleys])
    _peaks_n_valleys = _peaks_n_valleys.sort_index()
    _peaks_n_valleys.to_csv(f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip', compression='zip')
    plot_multi_timeframe_peaks_n_valleys(_peaks_n_valleys)
    # return _peaks_n_valleys


def read_multi_timeframe_peaks_n_valleys(date_range_str: str = config.under_process_date_range):
    return read_file(date_range_str, 'multi_timeframe_peaks_n_valleys', generate_multi_timeframe_peaks_n_valleys)
