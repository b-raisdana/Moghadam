import pandas as pd
import plotly
from bs4 import BeautifulSoup, Tag, NavigableString
from plotly import graph_objects as plgo
from plotly.io import to_html

from Config import config

DEBUG = False


def plot_ohlc(data: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
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


def batch_plot_to_html(figures: [plgo.Figure], file_name, file_open_mode='+w', show=True):
    # todo: test batch_plot_to_html
    # concatenated_body = ''
    # body_children_of_figures = [BeautifulSoup(figure).body.findChildren() for figure in figures.to_html]
    # final_figure = BeautifulSoup(figures[0])
    # for child in body_children_of_figures:
    #     final_figure.body.append(copy.copy(child))
    # output_html = figures[0].to_html()
    output_html = '<html><head></head><body>'
    for fig in figures:
        output_html += plotly.io.to_html(fig, full_html=False)
    output_html += '</body></html>'
    with open(file_name, file_open_mode) as f:
        f.write(output_html)
    if show:
        raise Exception('Not implemented')

# def clone(el):
#     if isinstance(el, NavigableString):
#         return type(el)(el)
#
#     copy = Tag(None, el.builder, el.name, el.namespace, el.nsprefix)
#     # work around bug where there is no builder set
#     # https://bugs.launchpad.net/beautifulsoup/+bug/1307471
#     copy.attrs = dict(el.attrs)
#     for attr in ('can_be_empty_element', 'hidden'):
#         setattr(copy, attr, getattr(el, attr))
#     for child in el.contents:
#         copy.append(clone(child))
#     return copy