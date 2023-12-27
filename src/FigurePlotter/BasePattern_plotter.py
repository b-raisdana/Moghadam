from typing import List

from pandera import typing as pt
from plotly import graph_objects as plgo

from BasePattern import timeframe_effective_bases
from Config import config
from FigurePlotter.OHLVC_plotter import plot_ohlcva
from FigurePlotter.plotter import file_id, save_figure, plot_multiple_figures
from Model.BasePattern import BasePattern, MultiTimeframeBasePattern
from Model.OHLCV import OHLCV
from Model.OHLCVA import MultiTimeframeOHLCVA
from data_preparation import single_timeframe
from helper import measure_time

MAX_NUMBER_OF_PLOT_SCATTERS = 5000


def plot_single_timeframe_base_pattern(single_timeframe_ohlcva: pt.DataFrame[OHLCV],
                                       timeframe_base_patterns: pt.DataFrame[BasePattern],
                                       name: str = '', show: bool = True,
                                       html_path: str = '', save: bool = True) -> plgo.Figure:
    fig: plgo.Figure = plot_ohlcva(single_timeframe_ohlcva, name=name, show=False, save=False)
    # remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    timeframe_base_patterns['effective_end'] = timeframe_base_patterns[['end', 'ttl']].min(axis=1, skipna=True)
    ohlcva_end = single_timeframe_ohlcva.index[-1]
    timeframe_base_patterns.loc[timeframe_base_patterns['effective_end'] > ohlcva_end, 'effective_end'] = ohlcva_end
    assert timeframe_base_patterns['effective_end'].notna().all()
    for (timeframe, _start), base_pattern in timeframe_base_patterns.iterrows():
        xs = [_start, base_pattern['effective_end'], base_pattern['effective_end'], _start]
        ys = [base_pattern['internal_low'], base_pattern['internal_low'], \
              base_pattern['internal_high'], base_pattern['internal_high']]
        fill_color = 'blue'
        name = BasePattern.name_repr(_start, timeframe, base_pattern)
        text = BasePattern.full_repr(_start, timeframe, base_pattern)
        fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                        fillpattern=dict(fgopacity=0.5),
                        name=name,
                        text=text,
                        line=dict(color=fill_color, width=0),
                        mode='lines',
                        legendgroup=name,
                        hoverinfo='text'
                        )
        if base_pattern['below_band_activated'] is not None:
            # add a vertical line equal to ATR of BasePattern at the time price chart goes 1 ATR under below edge.
            xs = [base_pattern['below_band_activated'], base_pattern['below_band_activated']]
            ys = [base_pattern['internal_low'], base_pattern['internal_low'] - base_pattern['ATR']]
            fig.add_scatter(x=xs, y=ys,
                            name=name,
                            text=text,
                            line=dict(color=fill_color, width=1),
                            mode='lines',  # +text',
                            showlegend=False,
                            legendgroup=name,
                            hovertemplate="%{text}",
                            )
        if base_pattern['below_band_activated'] is not None:
            # add a vertical line equal to ATR of BasePattern at the time price chart goes 1 ATR above upper edge.
            xs = [base_pattern['upper_band_activated'], base_pattern['upper_band_activated']]
            ys = [base_pattern['internal_high'], base_pattern['internal_high'] + base_pattern['ATR']]
            fig.add_scatter(x=xs, y=ys,
                            name=name,
                            text=text,
                            line=dict(color=fill_color, width=1),
                            mode='lines',  # +text',
                            showlegend=False,
                            legendgroup=name,
                            hovertemplate="%{text}",
                            )

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
            single_timeframe(multi_timeframe_ohlcva, timeframe),
            timeframe_effective_bases(_multi_timeframe_base_pattern, timeframe),
            show=False, save=False, name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, name=f'multi_timeframe_base_pattern.'
                                        f'{multi_timeframe_ohlcva.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                        f'{multi_timeframe_ohlcva.index[-1][1].strftime("%y-%m-%d.%H-%M")}',
                          show=show, save=save)
