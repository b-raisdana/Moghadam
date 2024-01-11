from typing import List, Union

import pandas as pd
from pandera import typing as pt
from plotly import graph_objects as plgo

from BasePattern import timeframe_effective_bases
from Config import config
from FigurePlotter.OHLVC_plotter import plot_ohlcva
from FigurePlotter.plotter import file_id, save_figure, plot_multiple_figures
from PanderaDFM.BasePattern import BasePattern, MultiTimeframeBasePattern
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.OHLCVA import MultiTimeframeOHLCVA
from helper.data_preparation import single_timeframe
from helper.helper import measure_time

MAX_NUMBER_OF_PLOT_SCATTERS = 5000


def plot_single_timeframe_base_pattern(single_timeframe_ohlcva: pt.DataFrame[OHLCV],
                                       timeframe_base_patterns: pt.DataFrame[BasePattern],
                                       base_ohlcv: Union[pt.DataFrame[OHLCV] | None],
                                       timeframe: str,
                                       name: str = '', show: bool = True,
                                       html_path: str = '', save: bool = True) -> plgo.Figure:
    if len(single_timeframe_ohlcva) == 0:
        return plot_ohlcva(single_timeframe_ohlcva, name=name, show=False, save=False)
    fig: plgo.Figure = plot_ohlcva(single_timeframe_ohlcva, base_ohlcv=base_ohlcv, name=name, show=False, save=False)
    # remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    timeframe_base_patterns['effective_end'] = timeframe_base_patterns[['end', 'ttl']].min(axis=1, skipna=True)
    ohlcva_end = single_timeframe_ohlcva.index[-1] + pd.to_timedelta(timeframe)
    timeframe_base_patterns.loc[timeframe_base_patterns['effective_end'] > ohlcva_end, 'effective_end'] = ohlcva_end
    assert timeframe_base_patterns['effective_end'].notna().all()
    for (timeframe, index_date), base_pattern in timeframe_base_patterns.iterrows():
        start = index_date + \
                pd.to_timedelta(timeframe) * config.base_pattern_index_shift_after_last_candle_in_the_sequence
        # mid_level = (base_pattern['internal_high'] + base_pattern['internal_low']) / 2
        xs = [start, base_pattern['effective_end'], base_pattern['effective_end'],
              start]
        ys = [base_pattern['internal_low'], base_pattern['internal_low'], \
              base_pattern['internal_high'], base_pattern['internal_high']]
        fill_color = 'blue'
        name = MultiTimeframeBasePattern.str(index_date, timeframe, base_pattern)
        text = MultiTimeframeBasePattern.repr(index_date, timeframe, base_pattern)
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
            # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr under below edge.
            xs = [start, base_pattern['below_band_activated']]
            ys = [base_pattern['internal_low'], base_pattern['internal_high'] + base_pattern['atr']]
            fig.add_scatter(x=xs, y=ys,
                            name=name,
                            text=text,
                            line=dict(color='yellow', width=1),
                            mode='lines',  # +text',
                            showlegend=False,
                            legendgroup=name,
                            hovertemplate="%{text}",
                            )
        if base_pattern['upper_band_activated'] is not None:
            # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr above upper edge.
            xs = [start, base_pattern['upper_band_activated']]
            ys = [base_pattern['internal_high'], base_pattern['internal_low'] - base_pattern['atr']]
            fig.add_scatter(x=xs, y=ys,
                            name=name,
                            text=text,
                            line=dict(color='yellow', width=1),
                            mode='lines',  # +text',
                            showlegend=False,
                            legendgroup=name,
                            hovertemplate="%{text}",
                            )
    fig.update_layout({
        'width': 1800,  # Set the width of the plot
        'height': 900,
        'legend': {
            'font': {
                'size': 8
            }
        }
    })
    # go.Layout(
    #     legend=dict(
    #         font=dict(
    #             size=16  # Set the font size for the legend text
    #         )
    #     )
    # )
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
    base_ohlcv = single_timeframe(multi_timeframe_ohlcva, config.timeframes[0])
    for timeframe in timeframe_shortlist:
        if timeframe != config.timeframes[0]:
            _figure = plot_single_timeframe_base_pattern(
                single_timeframe(multi_timeframe_ohlcva, timeframe),
                timeframe_effective_bases(_multi_timeframe_base_pattern, timeframe).copy(),
                base_ohlcv=base_ohlcv, timeframe=timeframe,
                show=False, save=False, name=f'{timeframe} boundaries')
        else:
            _figure = plot_single_timeframe_base_pattern(
                single_timeframe(multi_timeframe_ohlcva, timeframe),
                timeframe_effective_bases(_multi_timeframe_base_pattern, timeframe).copy(),
                base_ohlcv=None, timeframe=timeframe,
                show=False, save=False, name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, name=f'multi_timeframe_base_pattern.'
                                        f'{multi_timeframe_ohlcva.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                        f'{multi_timeframe_ohlcva.index[-1][1].strftime("%y-%m-%d.%H-%M")}',
                          show=show, save=save)
