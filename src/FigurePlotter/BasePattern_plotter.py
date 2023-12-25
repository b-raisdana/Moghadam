from typing import List

from pandera import typing as pt
from plotly import graph_objects as plgo

from Config import TREND, config
from FigurePlotter.OHLVC_plotter import plot_ohlcva
from FigurePlotter.plotter import file_id, save_figure, plot_multiple_figures
from Model.BasePattern import BasePattern, MultiTimeframeBasePattern
from Model.BullBearSide import bull_bear_side_repr
from Model.OHLCV import OHLCV
from Model.OHLCVA import MultiTimeframeOHLCVA
from PeakValley import major_peaks_n_valleys
from data_preparation import single_timeframe
from helper import measure_time, log

MAX_NUMBER_OF_PLOT_SCATTERS = 5000


def plot_single_timeframe_base_pattern(single_timeframe_ohlcva: pt.DataFrame[OHLCV],
                                       timeframe_base_patterns: pt.DataFrame[BasePattern],
                                       name: str = '', show: bool = True,
                                       html_path: str = '', save: bool = True) -> plgo.Figure:
    fig: plgo.Figure = plot_ohlcva(single_timeframe_ohlcva, name=name, show=False, save=False)
    # remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    for _start, base_pattern in timeframe_base_patterns.iterrows():
        xs = [_start] + base_pattern['end'] + [base_pattern['end']] + [_start]
        ys = base_pattern['internal_low'] + base_pattern['internal_low'] \
             + base_pattern['internal_high'] + base_pattern['internal_high']
        fill_color = 'blue'
        text = BasePattern.repr(_start, base_pattern)
        fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                        fillpattern=dict(fgopacity=0.5, shape='.'),
                        name=f'{base_pattern["bull_bear_side"].replace("_TREND", "")}: '
                             f'{_start.strftime("%H:%M")}-{base_pattern["end"].strftime("%H:%M")}',
                        line=dict(color=fill_color, width=0),
                        mode='lines',  # +text',
                        legendgroup='BasePattern',
                        )
        if base_pattern['below_band_activated'] is not None:
            # add a vertical line equal to ATR of BasePattern at the time price chart goes 1 ATR under below edge.
            xs = [base_pattern['below_band_activated'], base_pattern['below_band_activated']]
            ys = [base_pattern['internal_low'], base_pattern['internal_low'] - base_pattern['atr']]
            fig.add_scatter(x=xs, y=ys,  # fill="toself",  # fillcolor=fill_color,
                            # fillpattern=dict(fgopacity=0.5, shape='.'),
                            name=text,
                            line=dict(color=fill_color, width=0),
                            mode='lines',  # +text',
                            legendgroup='BasePattern-Bellow',
                            )
        if base_pattern['below_band_activated'] is not None:
            # add a vertical line equal to ATR of BasePattern at the time price chart goes 1 ATR above upper edge.
            xs = [base_pattern['upper_band_activated'], base_pattern['upper_band_activated']]
            ys = [base_pattern['internal_high'], base_pattern['internal_high'] + base_pattern['atr']]
            fig.add_scatter(x=xs, y=ys,  # fill="toself",  # fillcolor=fill_color,
                            # fillpattern=dict(fgopacity=0.5, shape='.'),
                            name=text,
                            line=dict(color=fill_color, width=0),
                            mode='lines',  # +text',
                            legendgroup='BasePattern-Upper',
                            )
        break

    if save or html_path != '':
        file_name = f'single_timeframe_base_pattern.{file_id(single_timeframe_ohlcva, name)}'
        save_figure(fig, file_name, html_path)

    if show: fig.show()
    return fig


@measure_time
def plot_multi_timeframe_base_pattern(multi_timeframe_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA],
                                      _multi_timeframe_base_pattern: pt.DataFrame[MultiTimeframeBasePattern],
                                      show: bool = True, save: bool = True, timeframe_shortlist: List['str'] = None) \
        -> None:
    figures = []
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        _figure = plot_single_timeframe_base_pattern(
            single_timeframe(multi_timeframe_ohlcva, timeframe), _multi_timeframe_base_pattern,
            show=False, save=False, name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, name=f'multi_timeframe_base_pattern.'
                                        f'{multi_timeframe_ohlcva.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                        f'{multi_timeframe_ohlcva.index[-1][1].strftime("%y-%m-%d.%H-%M")}',
                          show=show, save=save)
