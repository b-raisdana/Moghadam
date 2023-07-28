import os
import pandas as pd
from plotly import graph_objects as plgo
from plotly.subplots import make_subplots

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


def plot_multiple_figures(figures: [plgo.Figure], file_name, to_html=True, show=True,
                          path_of_plot: str = config.path_of_plots):
    """
    Save multiple Plotly figures to an HTML file.

    Parameters:
        figures (list): A list of Plotly figure objects to be saved.
        file_name (str): The name of the output HTML file.
        show (bool): If True, displays the plots in the browser.
        to_html: If True, saves the plots as html file.
        path_of_plot: Optional to specify the path of the plots. Used for easier test cleanup.
    Returns:
        None

    Sample usage:
        Suppose you have a list of Plotly figure objects named 'figures'
        batch_plot_to_html(figures, file_name='multi_timeframe_trend_boundaries.html', show=False)
    """
    # todo: test batch_plot_to_html
    # Create subplots with shared x-axis for better layout
    rows = len(figures)
    fig = make_subplots(rows=rows, cols=1, shared_xaxes=True)

    # Add each figure to the subplots
    for i, figure in enumerate(figures):
        for trace in figure['data']:
            fig.add_trace(trace, row=i + 1, col=1)

    # Update the layout to avoid overlap of subplots
    fig.update_layout(height=300 * rows, showlegend=False)

    # Set x-axis title for the last subplot
    fig.update_xaxes(title_text="Time", row=rows, col=1)

    # Show or save the HTML file
    if show:
        fig.show()
    if to_html:
        if not os.path.exists(path_of_plot):
            os.mkdir(path_of_plot)
        file_path = os.path.join(path_of_plot, file_name)
        fig.write_html(file_path)

    return fig
    # # concatenated_body = ''
    # # body_children_of_figures = [BeautifulSoup(figure).body.findChildren() for figure in figures.to_html]
    # # final_figure = BeautifulSoup(figures[0])
    # # for child in body_children_of_figures:
    # #     final_figure.body.append(copy.copy(child))
    # # output_html = figures[0].to_html()
    # output_html = '<html><head></head><body>'
    # for fig in figures:
    #     output_html += plotly.io.to_html(fig, full_html=False)
    # output_html += '</body></html>'
    # with open(file_name, file_open_mode) as f:
    #     f.write(output_html)
    # if show:
    #     raise Exception('Not implemented')

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
