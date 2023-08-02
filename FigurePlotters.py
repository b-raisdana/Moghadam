import os
from typing import List

from IPython.core.display_functions import display
from plotly import graph_objects as plgo

from Config import config

DEBUG = False


def plot_multiple_figures(figures: List[plgo.Figure], file_name: str, save: bool = True, show: bool = True,
                          path_of_plot: str = config.path_of_plots):
    figures_html = []
    for i, figure in enumerate(figures):
        figures_html.append(figure.to_html())

    combined_html = '<html><head></head><body>'
    for i, figure_html in enumerate(figures_html):
        combined_html += figure_html
    combined_html += '</body></html>'

    if save:
        file_path = os.path.join(path_of_plot,f'{file_name}.html' )
        with open(file_path, "w") as file:
            file.write(combined_html)
    if show: display(combined_html, raw=True, clear=True)  # Show the final HTML in the browser

    return combined_html

