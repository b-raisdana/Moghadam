import pandas as pd
from pandas import Timestamp

from Config import TopTYPE, config, TREND
from DataPreparation import read_file, read_multi_timeframe_ohlc, \
    single_timeframe
from FigurePlotters import batch_plot_to_html
from PeaksValleys import plot_peaks_n_valleys, peaks_only, valleys_only, read_multi_timeframe_peaks_n_valleys


def insert_previous_n_next_tops(single_timeframe_peaks_n_valleys, ohlc):
    # Todo: Not tested!
    # any 5min top is major to 1min candles so following condition is not right!
    # if len(peaks_n_valleys['timeframe'].unique()) > 1:
    #     raise Exception('Expected the peaks and valleys be grouped and filtered to see the same value for all rows.')
    ohlc = insert_previous_n_next_top(TopTYPE.PEAK, single_timeframe_peaks_n_valleys, ohlc)
    ohlc = insert_previous_n_next_top(TopTYPE.VALLEY, single_timeframe_peaks_n_valleys, ohlc)
    return ohlc


def insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlc):
    # Todo: Not tested!
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value]
    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
    for i in range(len(tops)):
        # if i == len(tops) - 1:
        #     for j in ohlc.loc[tops.index[i] < ohlc.index].index:
        #         ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
        #         ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        # else:
        #     for j in ohlc.loc[(tops.index[i] < ohlc.index) & (ohlc.index <= tops.index[i + 1])].index:
        #         ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
        #         ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        #     if i == 0:
        #         for j in ohlc.loc[ohlc.index <= tops.index[i]].index:
        #             ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
        #             ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        #     else:
        #         for j in ohlc.loc[(tops.index[i - 1] < ohlc.index) & (ohlc.index <= tops.index[i])].index:
        #             ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
        #             ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        if i == len(tops) - 1:
            indexes = ohlc.loc[tops.index[i] < ohlc.index].index
            ohlc.loc[indexes, f'previous_{top_type.value}_index'] = tops.index[i]
            ohlc.loc[indexes, f'previous_{top_type.value}_value'] = tops.iloc[i][
                high_or_low]
        else:
            indexes = ohlc.loc[(tops.index[i] < ohlc.index) & (ohlc.index <= tops.index[i + 1])].index
            ohlc.loc[indexes, f'previous_{top_type.value}_index'] = tops.index[i]
            ohlc.loc[indexes, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        if i == 0:
            indexes = ohlc.loc[ohlc.index <= tops.index[i]].index
            ohlc.loc[indexes, f'next_{top_type.value}_index'] = tops.index[i]
            ohlc.loc[indexes, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        else:
            indexes = ohlc.loc[(tops.index[i - 1] < ohlc.index) & (ohlc.index <= tops.index[i])].index
            ohlc.loc[indexes, f'next_{top_type.value}_index'] = tops.index[i]
            ohlc.loc[indexes, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
    return ohlc


def single_timeframe_candles_trend(ohlc: pd.DataFrame, single_timeframe_peaks_n_valley: pd.DataFrame) -> pd.DataFrame:
    # Todo: Not tested!
    # if timeframe not in config.timeframes:
    #     raise Exception(f'Unsupported timeframe:{timeframe} expected to be from: [{config.timeframes}]')
    # _higher_or_eq_timeframe_peaks_n_valleys = higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valley, timeframe)
    # if any([i not in ohlc.columns for i in
    #         [f'{j}_{k}' for j in ['previous', 'next'] for k in [e.value for e in TopTYPE]]]):
    #     ohlc = insert_previous_n_next_peaks_n_valleys(_higher_or_eq_timeframe_peaks_n_valleys, ohlc)
    # ohlc[f'bull_bear_side_{timeframe}'] = TREND.SIDE
    # ohlc.loc[
    #     (ohlc['next_valley_value'] > ohlc['previous_valley_value']) &
    #     (ohlc['next_peak_value'] > ohlc['previous_peak_value']) &
    #     (ohlc['next_peak_index'] > ohlc['next_valley_index'])  # the higher peak should be after higher valley
    #     , f'bull_bear_side_{timeframe}'] = TREND.BULLISH
    # ohlc.loc[
    #     (ohlc['next_peak_value'] < ohlc['previous_peak_value']) &
    #     (ohlc['next_valley_value'] < ohlc['previous_valley_value']) &
    #     (ohlc['next_peak_index'] > ohlc['next_valley_index'])  # the lower valley should be after lower peak
    #     , f'bull_bear_side_{timeframe}'] = TREND.BEARISH
    # return ohlc
    # if timeframe not in config.timeframes:
    #     raise Exception(f'Unsupported timeframe:{timeframe} expected to be from: [{config.timeframes}]')
    _previous_n_next_tops = insert_previous_n_next_tops(single_timeframe_peaks_n_valley, ohlc)
    candle_trend = pd.DataFrame([TREND.SIDE.value] * len(_previous_n_next_tops), index=_previous_n_next_tops.index,
                                columns=['bull_bear_side'])
    candle_trend.loc[_previous_n_next_tops.index[
        (_previous_n_next_tops['next_valley_value'] > _previous_n_next_tops['previous_valley_value']) &
        (_previous_n_next_tops['next_peak_value'] > _previous_n_next_tops['previous_peak_value']) &
        (_previous_n_next_tops['next_peak_index'] > _previous_n_next_tops[
            'next_valley_index'])]  # the higher peak should be after higher valley
    , 'bull_bear_side'] = TREND.BULLISH.value
    candle_trend.loc[_previous_n_next_tops.index[
        (_previous_n_next_tops['next_peak_value'] < _previous_n_next_tops['previous_peak_value']) &
        (_previous_n_next_tops['next_valley_value'] < _previous_n_next_tops['previous_valley_value']) &
        (_previous_n_next_tops['next_peak_index'] > _previous_n_next_tops[
            'next_valley_index'])]  # the lower valley should be after lower peak
    , 'bull_bear_side'] = TREND.BEARISH.value
    return candle_trend


def multi_timeframe_trend_boundaries(multi_timeframe_candle_trend: pd.DataFrame,
                                     multi_timeframe_peaks_n_valleys: pd.DataFrame):
    # self.multi_timeframe_trend_boundaries_columns = ['timeframe', 'end', 'bull_bear_side',
    #                                                  'highest_hig', 'lowest_low', 'high_time', 'low_time',
    #                                                  'trend_line_acceleration', 'trend_line_base',
    #                                                  'canal_line_acceleration', 'canal_line_base',
    #                                                  ]
    # timeframes = [i.replace('bull_bear_side_', '') for i in multi_timeframe_candle_trend.columns if
    #                    i.startswith('bull_bear_side_')]
    # for _, timeframe in enumerate(config.structure_timeframes):
    #     if f'previous_bull_bear_side_{timeframe}' not in multi_timeframe_candle_trend.columns:
    #         raise Exception(
    #             f'previous_bull_bear_side_{timeframe} not found in candle_trend:({multi_timeframe_candle_trend.columns})')
    # boundaries = pd.DataFrame()
    # multi_timeframe_candle_trend['time_of_previous'] = multi_timeframe_candle_trend.index.shift(-1, freq='1min')
    # for timeframe in timeframes:
    #     # todo: move effective time to a column to prevent multiple columns for different effective times.
    #     multi_timeframe_candle_trend.loc[timeframe, 'previous_bull_bear_side'] = multi_timeframe_candle_trend[
    #         f'bull_bear_side_{timeframe}'].shift(1)
    #     time_boundaries = multi_timeframe_candle_trend[
    #         multi_timeframe_candle_trend[f'previous_bull_bear_side_{timeframe}'] != multi_timeframe_candle_trend[
    #             f'bull_bear_side_{timeframe}']]
    #     time_boundaries['timeframe'] = timeframe
    #     time_boundaries['bull_bear_side'] = time_boundaries[f'bull_bear_side_{timeframe}']
    #     unnecessary_columns = [i for i in multi_timeframe_candle_trend.columns if
    #                            i.startswith('bull_bear_side_') or i.startswith('previous_bull_bear_side_')]
    #     time_boundaries.drop(columns=unnecessary_columns, inplace=True)
    #     time_of_last_candle = multi_timeframe_candle_trend.index[-1]
    #     time_boundaries.loc[:, 'time_of_next'] = time_boundaries.index
    #     time_boundaries.loc[:, 'time_of_next'] = time_boundaries['time_of_next'].shift(-1)
    #     time_boundaries.loc[time_boundaries.index[-1], 'time_of_next'] = time_of_last_candle
    #     time_boundaries.loc[:, 'end'] = multi_timeframe_candle_trend.loc[
    #         time_boundaries['time_of_next'], 'time_of_previous'].tolist()
    #     time_boundaries.drop(columns=['time_of_next'])
    #     boundaries = pd.concat([boundaries, time_boundaries])
    timeframes = multi_timeframe_candle_trend.index.unique(level=0)
    for timeframe in timeframes:
        single_timeframe_candle_trend = single_timeframe(multi_timeframe_candle_trend, timeframe)
        single_timeframe_peaks_n_valleys = single_timeframe(multi_timeframe_peaks_n_valleys)
        _timeframe_trend_boundaries = single_timeframe_trend_boundaries(single_timeframe_candle_trend,
                                                                        single_timeframe_peaks_n_valleys)
        _timeframe_trend_boundaries.set_index('timeframe', append=True, inplace=True)
        _timeframe_trend_boundaries = _timeframe_trend_boundaries.swaplevel()
        boundaries = pd.concat([boundaries, _timeframe_trend_boundaries])
    boundaries = boundaries[config.multi_timeframe_trend_boundaries_columns]
    return boundaries


def add_highest_high_n_lowest_low(_boundaries, single_timeframe_candle_trend):
    for i, _boundary in _boundaries.iterrows():
        _boundaries[i, 'highest_high'] = single_timeframe_candle_trend.loc[_boundary.index[i]:_boundary.loc[i, 'end'], \
                                         'high'].max()
        _boundaries[i, 'lowest_low'] = single_timeframe_candle_trend.loc[_boundary.index[i]:_boundary.loc[i, 'end'], \
                                       'low'].min()


def add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys):
    for i, _boundary in _boundaries.iterrows():
        if _boundary['bull_bear_side'] == TREND.SIDE.value:
            continue
        _peaks = most_two_significant_peaks(i, _boundary['end'], single_timeframe_peaks_n_valleys)
        _valleys = most_two_significant_valleys(i, _boundary['end'], single_timeframe_peaks_n_valleys)
        if _boundary['bull_bear_side'] == TREND.BULLISH.value:
            canal_tops = _peaks
            trend_peaks = _valleys
        else:
            canal_tops = _valleys
            trend_peaks = _peaks
        trend_base =
        trend_acceleration =
        canal_base =
        canal_acceleration =


def single_timeframe_trend_boundaries(single_timeframe_candle_trend: pd.DataFrame,
                                      single_timeframe_peaks_n_valleys) -> pd.DataFrame:
    _boundaries = detect_boundaries(single_timeframe_candle_trend)

    _boundaries = add_highest_high_n_lowest_low(_boundaries, single_timeframe_candle_trend)

    _boundaries = add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys)

    # ['end', 'bull_bear_side',
    #  'highest_hig', 'lowest_low', 'high_time', 'low_time',
    #  'trend_line_acceleration', 'trend_line_base',
    #  'canal_line_acceleration', 'canal_line_base',
    #  ]
    _boundaries = _boundaries[[i for i in config.multi_timeframe_trend_boundaries_columns if i != 'timeframe']]
    return _boundaries


def detect_boundaries(single_timeframe_candle_trend):
    single_timeframe_candle_trend.loc[1:, 'time_of_previous'] = single_timeframe_candle_trend.index[:-1]
    # single_timeframe_candle_trend['time_of_previous'] = single_timeframe_candle_trend.index.shift(-1, freq=timeframe)
    single_timeframe_candle_trend['previous_trend'] = single_timeframe_candle_trend['bull_bear_side'].shift(1)
    _boundaries = single_timeframe_candle_trend[
        single_timeframe_candle_trend['previous_trend'] != single_timeframe_candle_trend['bull_bear_side']]
    time_of_last_candle = single_timeframe_candle_trend.index[-1]
    # _boundaries['time_of_next'] = _boundaries.index.shift(-1, freq=timeframe)
    _boundaries.loc[:-1, 'end'] = _boundaries.index[1:]
    # _boundaries.loc[:, 'time_of_next'] = _boundaries['time_of_next'].shift(-1)
    _boundaries.loc[_boundaries.index[-1], 'end'] = time_of_last_candle
    # _boundaries['end'] = single_timeframe_candle_trend.loc[_boundaries['time_of_next'], 'time_of_previous'] \
    #     .tolist()
    return _boundaries[['bull_bear_side', 'end']]


def boundary_including_peaks_valleys(peaks_n_valleys: pd.DataFrame, boundary_start: pd.Timestamp,
                                     boundary_end: pd.Timestamp):
    return peaks_n_valleys.loc[(peaks_n_valleys.index >= boundary_start) & (peaks_n_valleys.index <= boundary_end)]


MAX_NUMBER_OF_PLOT_SCATTERS = 50


def plot_bull_bear_side(ohlc: pd.DataFrame, peaks_n_valleys: pd.DataFrame, boundaries: pd.DataFrame,
                        name: str = '', do_not_show: bool = False, html_path: str = ''):
    fig = plot_peaks_n_valleys(ohlc, peaks=peaks_only(peaks_n_valleys), valleys=valleys_only(peaks_n_valleys),
                               name=name, do_not_show=True)
    # if boundaries is None:
    #     boundaries = multi_timeframe_trend_boundaries(multi_timeframe_candle_trend)

    remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    for timeframe in boundaries['timeframe'].unique():
        if remained_number_of_scatters <= 0:
            break
        for boundary_index, boundary in boundaries.iterrows():
            if boundary_index == Timestamp('2017-01-04 11:17:00'):
                pass
            boundary_peaks_n_valleys = boundary_including_peaks_valleys(peaks_n_valleys, boundary_index,
                                                                        boundary['end'])
            boundary_peaks = peaks_only(boundary_peaks_n_valleys)
            boundary_valleys = valleys_only(boundary_peaks_n_valleys)
            xs = [boundary_index] + boundary_peaks.index.tolist() + [boundary['end']] + \
                 sorted(boundary_valleys.index.tolist(), reverse=True)
            ys = [ohlc.loc[boundary_index, 'open']] + boundary_peaks['high'].values.tolist() + \
                 [ohlc.loc[boundary['end'], 'close']] + sorted(boundary_valleys['low'].values.tolist(), reverse=True)
            fill_color = 'green' if boundary['bull_bear_side'] == TREND.BULLISH.value else \
                'red' if boundary['bull_bear_side'] == TREND.BEARISH.value else 'gray'
            if remained_number_of_scatters > 0:
                fig.add_scatter(x=xs, y=ys, mode="lines", fill="toself",  # fillcolor=fill_color,
                                fillpattern=dict(fgopacity=0.5, shape='.'),
                                name=f'{boundary_index} {timeframe}',
                                line=dict(color=fill_color, width=config.timeframes.index(timeframe) + 1))
                remained_number_of_scatters -= 1
            else:
                break

    if html_path != '':
        figure_as_html = fig.to_html()
        with open(html_path, '+w') as f:
            f.write(figure_as_html)
    if not do_not_show: fig.show()
    return fig


def read_multi_timeframe_trend_boundaries(date_range_str: str):
    return read_file(date_range_str, 'multi_timeframe_trend_boundaries', generate_multi_timeframe_trend_boundaries)


def plot_single_time_frame_trend_boundaries(param):
    # todo: implement plot_single_time_frame_trend_boundaries
    pass


def plot_multi_timeframe_trend_boundaries(multi_timeframe_trend_boundaries):
    # todo: implement plot_multi_timeframe_trend_boundaries
    figures = []
    for _, timeframe in enumerate(multi_timeframe_trend_boundaries['timeframe'].unique()):
        _figure = plot_single_time_frame_trend_boundaries(
            multi_timeframe_trend_boundaries[multi_timeframe_trend_boundaries['timeframe'] == timeframe])
        figures.append(_figure)
    batch_plot_to_html(figures)


def generate_multi_timeframe_trend_boundaries(date_range_str: str):
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    multi_timeframe_candle_trend = generate_multi_timeframe_candle_trend(date_range_str)
    trend_boundaries = multi_timeframe_trend_boundaries(multi_timeframe_candle_trend, multi_timeframe_peaks_n_valleys)
    trend_boundaries.to_csv(f'multi_timeframe_trend_boundaries.{date_range_str}.zip', compression='zip')
    plot_multi_timeframe_trend_boundaries(trend_boundaries)


# def read_multi_timeframe_candle_trend(date_range_str: str):
#     return read_file(date_range_str, 'multi_timeframe_candle_trend', generate_multi_timeframe_candle_trend)


def plot_multi_timeframe_candle_trend(multi_timeframe_candle_trend):
    # todo: implement plot_multi_timeframe_candle_trend
    pass


def generate_multi_timeframe_candle_trend(date_range_str: str):
    # def candles_trend_multi_timeframe(ohlca: pd.DataFrame, peaks_n_valleys: pd.DataFrame) -> pd.DataFrame:
    #     for timeframe in peaks_n_valleys['timeframe'].unique():
    #         ohlca = candles_trend_single_timeframe(timeframe, ohlca, peaks_n_valleys)
    #     return ohlca
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    multi_timeframe_candle_trend = pd.DataFrame()
    for timeframe in peaks_n_valleys.index.unique(level='timeframe'):
        # todo: we left here between multi_timeframe_candle_trend and candles_trend_single_timeframe!!!
        ohlc = single_timeframe(multi_timeframe_ohlc, timeframe)
        # _higher_or_eq_timeframe_peaks_n_valleys = peaks_n_valleys.loc[timeframe]
        _timeframe_candle_trend = single_timeframe_candles_trend(ohlc, peaks_n_valleys.loc[timeframe])
        _timeframe_candle_trend['timeframe'] = timeframe
        _timeframe_candle_trend.set_index('timeframe', append=True, inplace=True)
        _timeframe_candle_trend = _timeframe_candle_trend.swaplevel()
        multi_timeframe_candle_trend = pd.concat([multi_timeframe_candle_trend, _timeframe_candle_trend])
    # multi_timeframe_candle_trend = multi_timeframe_candle_trend.sort_index()
    return multi_timeframe_candle_trend
