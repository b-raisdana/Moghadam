import pandas as pd
import plotly.graph_objects as go
from plotly import graph_objects as plgo
from plotly.subplots import make_subplots

from Config import config, CandleSize
from Model.OHLCV import OHLCV
from data_preparation import single_timeframe
from FigurePlotter.plotter import plot_multiple_figures, file_id, DEBUG, save_figure
from helper import log, measure_time
from pandera import typing as pt

@measure_time
def plot_multi_timeframe_ohlcva(multi_timeframe_ohlcva, name: str = '', show: bool = True, save: bool = True) -> None:
    # todo: test plot_multi_timeframe_ohlcva
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlcva(single_timeframe(multi_timeframe_ohlcva, timeframe), show=False, save=False,
                                   name=f'{timeframe} ohlcva'))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlcva.{file_id(multi_timeframe_ohlcva, name)}',
                          save=save, show=show)


@measure_time
def plot_multi_timeframe_ohlcv(multi_timeframe_ohlcv, date_range_str, show: bool = True, save: bool = True):
    # todo: test plot_multi_timeframe_ohlcv
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlcv(single_timeframe(multi_timeframe_ohlcv, timeframe), show=False, save=False,
                                  name=f'{timeframe} ohlcv'))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlcv.{date_range_str}', show=show, save=save)


@measure_time
def plot_ohlcv(ohlcv: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
               # plot_base_ohlcv: bool = False,
               base_ohlcv: pt.DataFrame[OHLCV] = None,
               save: bool = True, name: str = '', show: bool = True) -> plgo.Figure:
    """
        Plot OHLC (Open, High, Low, Close) data as a candlestick chart.

        Parameters:
            ohlcv (pd.DataFrame): A DataFrame containing OHLC data.
            save (bool): If True, the plot is saved as an image file.
            name (str): The name of the plot.
            show (bool): If False, the plot will not be displayed.

        Returns:
            plgo.Figure: The Plotly figure object containing the OHLC candlestick chart.
            :param show:
            :param name:
            :param save:
            :param ohlcv:
            :param base_ohlcv:
            :param plot_base_ohlcv:
        """

    if DEBUG: log(f'data({ohlcv.shape})')
    if DEBUG: log(ohlcv)
    fig = plgo.Figure(data=[plgo.Candlestick(x=ohlcv.index.values,
                                             open=ohlcv['open'], high=ohlcv['high'], low=ohlcv['low'],
                                             close=ohlcv['close']
                                             , name=name
                                             )]).update_yaxes(fixedrange=False).update_layout(yaxis_title=name)
    if base_ohlcv is not None:
        fig.add_candlestick(
            x=base_ohlcv.index.values,
            open=base_ohlcv['open'], high=base_ohlcv['high'], low=base_ohlcv['low'],
            close=base_ohlcv['close']
            , name=config.timeframes[0]
        )
    if show: fig.show()
    if save:
        file_name = f'ohlcv.{file_id(ohlcv, name)}'
        save_figure(fig, file_name)

    return fig


@measure_time
def plot_ohlcva(ohlcva: pd.DataFrame, save: bool = True, show: bool = True, name: str = '',
                base_ohlcv: pt.DataFrame[OHLCV] = None) -> plgo.Figure:
    """
    Plot OHLC data with an additional ATR (Average True Range) boundary.

    The function plots OHLC data as a candlestick chart and adds an ATR boundary to the plot.
    The boundary's middle is calculated as the average of the candle's open and close,
    and the width of the boundary is equal to the ATR value for each data point.

    Parameters:
        ohlcva (pd.DataFrame): A DataFrame containing OHLC data along with the 'ATR' column representing the ATR values.
        save (bool): If True, the plot is saved as an HTML file.
        show (bool): If True, the plot is displayed in the browser.

    Returns:
        None

    Example:
        # Assuming you have the 'ohlcva' DataFrame with the required columns (open, high, low, close, ATR)
        date_range_str = "17-10-06.00-00T17-10-06"
        plot_ohlcva(ohlcva, date_range_str)
        :param save:
        :param base_ohlcv:
        :param show:
        :param ohlcva:
        :param name:
    """
    # Create a figure with 2 rows
    master_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02,
                               row_heights=[0.7, 0.3])  # Adjust row heights as needed
    # Calculate the middle of the boundary (average of open and close)
    midpoints = (ohlcva['high'] + ohlcva['low']) / 2
    # Create a figure using the plot_ohlcv function
    sub_fig = plot_ohlcv(ohlcva[['open', 'high', 'low', 'close']], base_ohlcv, show=False, save=False, name=name)

    # Add the ATR boundaries
    sub_fig = add_atr_scatter(sub_fig, ohlcva.index, midpoints=midpoints,
                              widths=CandleSize.Spinning.value.max * ohlcva['ATR'],
                              name='Standard')
    sub_fig = add_atr_scatter(sub_fig, ohlcva.index, midpoints=midpoints,
                              widths=CandleSize.Standard.value.max * ohlcva['ATR'],
                              name='Long')
    sub_fig = add_atr_scatter(sub_fig, ohlcva.index, midpoints=midpoints,
                              widths=CandleSize.Long.value.max * ohlcva['ATR'],
                              name='Spike')

    sub_fig.add_scatter(x=ohlcva.index, y=midpoints,
                        mode='none',
                        showlegend=False,
                        text=[f'ATR: {atr:0.1f}' for atr in ohlcva['ATR']],
                        hoverinfo='text')

    sub_fig.update_layout(hovermode='x unified')

    for trace in sub_fig.data:
        master_fig.add_trace(trace, row=1, col=1)

    master_fig.add_trace(go.Scatter(
        x=ohlcva.index,
        y=ohlcva['ATR'],
        name='ATR',
        mode='lines',  # Use line mode
        line=dict(color='black')), row=2, col=1)

    # Update layout and Aesthetics
    master_fig.update_layout(
        xaxis=dict(
            fixedrange=False,  # Allows zooming on the x-axis
            rangeslider=dict(visible=False)  # Hide range slider for the first subplot (price chart)
        ),
        xaxis2=dict(
            fixedrange=False,  # Allows zooming on the x-axis
            rangeslider=dict(visible=True)  # Show range slider for the second subplot (ATR chart)
        ),
        yaxis=dict(
            fixedrange=False  # Allows zooming on the y-axis for the first subplot
        ),
        yaxis2=dict(
            fixedrange=False  # Allows zooming on the y-axis for the second subplot
        ),
        height=800, width=1200, title_text=name)
    master_fig.update_yaxes(title_text="Price", row=1, col=1)
    master_fig.update_yaxes(title_text="ATR", row=2, col=1)

    # Show the figure or write it to an HTML file
    if save:
        file_name = f'ohlcva.{file_id(ohlcva, name)}'
        save_figure(master_fig, file_name)

    if show:
        master_fig.show()
    return master_fig


def add_atr_scatter(fig: plgo.Figure, xs: pd.Series, midpoints: pd.Series, widths: pd.Series,
                    transparency: float = 0.2, name: str = 'ATR') -> plgo.Figure:
    xs = xs.tolist()
    half_widths = widths.fillna(value=0).div(2)
    upper_band: pd.Series = midpoints + half_widths
    lower_band: pd.Series = midpoints - half_widths

    return fig.add_trace(
        plgo.Scatter(
            x=xs + xs[::-1],
            y=upper_band.tolist() + lower_band.tolist()[::-1],
            mode='lines',
            # hovertext=widths,
            line=dict(color='gray', dash='solid', width=0.2),
            fill='toself',
            fillcolor=f'rgba(128, 128, 128, {transparency})',  # 50% transparent gray color
            name=name
        )
    )
