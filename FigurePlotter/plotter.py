import colorsys
import os
import webbrowser
from typing import List, Tuple

import pandas as pd
from plotly import graph_objects as plgo

from Config import config
from DataPreparation import range_of_data
from helper import measure_time

DEBUG = False


# @measure_time
def plot_multiple_figures(figures: List[plgo.Figure], name: str, save: bool = True, show: bool = True,
                          path_of_plot: str = config.path_of_plots):
    figures_html = []
    for i, figure in enumerate(figures):
        figures_html.append(figure.to_html())

    combined_html = '<html><head></head><body>'
    for i, figure_html in enumerate(figures_html):
        combined_html += figure_html
    combined_html += '</body></html>'

    file_path = os.path.join(path_of_plot, f'{name}.html')
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(combined_html)
    if show:
        full_path = os.path.abspath(file_path)
        webbrowser.register('firefox',
                            None,
                            webbrowser.BackgroundBrowser("C://Program Files//Mozilla Firefox//firefox.exe"))
        webbrowser.get('firefox').open(f'file://{full_path}')
        # display(combined_html, raw=True, clear=True)  # Show the final HTML in the browser
    if not save: os.remove(combined_html)

    return combined_html


def save_figure(fig: plgo.Figure, file_name: str, file_path: str = '') -> None:
    """
    Save a Plotly figure as an HTML file.

    Parameters:
        fig (plotly.graph_objects.Figure): The Plotly figure to be saved.
        file_name (str): The name of the output HTML file (without extension).
        file_path (str, optional): The path to the directory where the HTML file will be saved.
                                  If not provided, the default path will be used.

    Returns:
        None

    Example:
        # Assuming you have a Plotly figure 'fig' and want to save it as 'my_plot.html'
        save_figure(fig, file_name='my_plot')

    Note:
        This function uses the Plotly 'write_html' method to save the figure as an HTML file.
    """
    if file_path == '':
        file_path = config.path_of_plots
    if not os.path.exists(file_path):
        os.mkdir(file_path)

    file_path = os.path.join(file_path, f'{file_name}.html')
    fig.write_html(file_path)


def file_id(data: pd.DataFrame, name: str = '') -> str:
    """
        Generate a file identifier based on data's date range and an optional name.

        This function generates a file identifier using the data's date range and an optional name parameter.
        If the name parameter is not provided or is empty, the file identifier will consist of the date range only.
        If a name parameter is provided, it will be appended to the beginning of the file identifier.

        Parameters:
            data (pd.DataFrame): The DataFrame for which to generate the file identifier.
            name (str, optional): An optional name to be included in the file identifier.

        Returns:
            str: The generated file identifier.

        Example:
            # Assuming you have a DataFrame 'data' and want to generate a file identifier
            identifier = file_id(data, name='my_data')
            print(identifier)  # Output: 'my_data.yy-mm-dd.HH-MMTyy-mm-dd.HH-MM'
        """
    if name is None or name == '':
        return f'{range_of_data(data)}'
    else:
        return f'{name}.{range_of_data(data)}'


def timeframe_color(timeframe: str) -> str:

    h = (config.timeframes.index(timeframe)*20 + 120) % 360
    s, b = (1, 1)
    r, g, b = [int(x * 255) for x in colorsys.hsv_to_rgb(h / 360, s, b)]
    return f'rgb({r},{g},{b})'
