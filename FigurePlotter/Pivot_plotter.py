from pandera import typing as pt

from Model.MultiTimeframePivot import MultiTimeframePivot
from Candle import read_ohlca
from Config import config
from FigurePlotter.DataPreparation_plotter import plot_ohlca

"""
class Pivot(pandera.DataFrameModel):
    date: pt.Index[datetime]
    movement_start_time: pt.Series[datetime]
    movement_start_value: pt.Series[datetime]
    return_end_time: pt.Series[datetime]
    return_end_value: pt.Series[datetime]
    level: pt.Series[float]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    is_active: pt.Series[bool]
    hit: pt.Series[int]
    overlapped_with_major_timeframe: pt.Series[bool]


class MultiTimeframePivot(Pivot, MultiTimeframe):
    pass
"""


def plot_multi_timeframe_bull_bear_side_pivots(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot],
                                               date_range_string: str = config.under_process_date_range):
    # Create the figure using plot_peaks_n_valleys function
    base_ohlca = read_ohlca(date_range_string)
    end_time = base_ohlca.index[-1]
    fig = plot_ohlca(ohlca=base_ohlca, show=False, save=False)

    for (pivot_timeframe, pivot_start), pivot_info in multi_timeframe_pivots.iterrows():
        pivot_name = (f"Pivot {pivot_timeframe}@{pivot_start}("
                      f"M{abs(pivot_info['movement_start_value'] - pivot_info['level'])}/"
                      f"R{abs(pivot_info['return_end_value'] - pivot_info['level'])})")
        # add movement and return paths
        fig.add_scatter(x=[
            pivot_info['movement_start_time'], pivot_start, pivot_info['return_end_time']],
            y=[pivot_info['movement_start_value'], pivot_info['level'], pivot_info['return_end_value']],
            name=pivot_name, line=dict(color='cyan'), mode='lines',  # +text',
        )

        # add a dotted line from creating time of level to the activation time
        fig.add_scatter(
            x=[pivot_start, pivot_info['activation_time']],
            y=[pivot_info['level'], pivot_info['level']],
            name=pivot_name, line=dict(color='blue', dash='dot'), mode='lines',  # +text',
        )
        # draw the level line
        if pivot_info['deactivation_time'] is None:
            level_end_time = end_time
        else:
            level_end_time = pivot_info['deactivation_time']
        fig.add_scatter(
            x=[pivot_info['activation_time'], level_end_time],
            y=[pivot_info['level'], pivot_info['level']],
            name=pivot_name, line=dict(color='blue'), mode='lines',  # +text',
        )
        # draw the level boundary
        fig.add_scatter(
            x=[pivot_info['activation_time'], level_end_time, level_end_time, pivot_info['activation_time']],
            y=[pivot_info['external_margin'], pivot_info['external_margin'],
               pivot_info['internal_margin'], pivot_info['internal_margin']],
            fill="toself", fillpattern=dict(fgopacity=0.5, shape='.'),
            name=pivot_name, line=dict(color='yellow'), mode='lines',  # +text',
        )
