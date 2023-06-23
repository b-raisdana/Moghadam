import pandas as pd
from plotly import graph_objects as plgo

from LevelDetectionConfig import config

DEBUG = False


def plot_ohlc_with_peaks_n_valleys(ohlc: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
                                   peaks: pd = pd.DataFrame(columns=['high', 'effective_time']),
                                   valleys: pd = pd.DataFrame(columns=['low', 'effective_time']),
                                   name: str = '') -> plgo.Figure:
    fig = plotfig(ohlc, name=name, save=False, do_not_show=True)
    if len(peaks) > 0:
        fig.add_scatter(x=peaks.index.values, y=peaks['high'] + 1, mode="markers", name='P',
                        marker=dict(symbol="triangle-up", color="blue"),
                        hovertemplate="%{text}",
                        text=[
                            f"{peaks.loc[_x]['effective_time']}@{peaks.loc[_x]['high']}"
                            for _x in peaks.index.values]
                        )
    if len(valleys) > 0:
        fig.add_scatter(x=valleys.index.values, y=valleys['low'] - 1, mode="markers", name='V',
                        marker=dict(symbol="triangle-down", color="blue"),
                        hovertemplate="%{text}",
                        text=[
                            f"{valleys.loc[_x]['effective_time']}@{valleys.loc[_x]['low']}"
                            for _x in valleys.index.values]
                        )
        fig.update_layout(hovermode='x unified')
        fig.show()
    return fig


def plotfig(data: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
            save: bool = False, name: str = '', do_not_show: bool = False) -> plgo.Figure:
    import os
    MAX_LEN_OF_DATA_FRAME_TO_PLOT = 50000
    SAFE_LEN_OF_DATA_FRAME_TO_PLOT = 10000
    if len(data.index) > MAX_LEN_OF_DATA_FRAME_TO_PLOT:
        raise Exception(f'Too many rows to plt ({len(data.index),}>{MAX_LEN_OF_DATA_FRAME_TO_PLOT})')
    if len(data.index) > SAFE_LEN_OF_DATA_FRAME_TO_PLOT:
        print(f'Plotting too much data will slow us down ({len(data.index),}>{SAFE_LEN_OF_DATA_FRAME_TO_PLOT})')
    if not os.path.isfile('kaleido.installed'):
        print('kaleido not satisfied!')
        try:
            os.system('pip install -q condacolab')
            import condacolab

            if not condacolab.check():
                condacolab.install()
                os.system('conda install -c conda-forge python-kaleido')
                os.system('echo "" > kaleido.installed')
            else:
                print('condacolab already satisfied')
        except:
            os.system('pip install -U kaleido')
            os.system('echo "" > kaleido.installed')
    if DEBUG: print(f'data({data.shape})')
    if DEBUG: print(data)
    fig = plgo.Figure(data=[plgo.Candlestick(x=data.index.values,
                                             open=data['open'], high=data['high'], low=data['low'],
                                             close=data['close'],
                                             )], ).update_yaxes(fixedrange=False).update_layout(yaxis_title=name)
    if not do_not_show: fig.show()
    if save: fig.write_image(f'{config.id}.{name}.png')

    return fig
