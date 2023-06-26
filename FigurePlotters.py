import pandas as pd
from plotly import graph_objects as plgo
from plotly.io import to_html

from Config import config

DEBUG = False


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


def concat_figures(figures: [plgo.Figure]):
    # todo: test it
    for figure in figures:
        print(to_html(figure))
