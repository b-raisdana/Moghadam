import pytest
import os
import shutil
import plotly.graph_objects as go

from Config import config
from FigurePlotter import plot_multiple_figures

# Create some sample Plotly figure objects for testing
fig1 = go.Figure(go.Scatter(x=[1, 2, 3], y=[4, 5, 6]))
fig2 = go.Figure(go.Bar(x=[1, 2, 3], y=[7, 8, 9]))
fig3 = go.Figure(go.Scatter(x=[1, 2, 3], y=[10, 11, 12]))
figures = [fig1, fig2, fig3]


# Define a fixture to clean up the config.path_of_test_plots='test_plots' directory after the tests
@pytest.fixture(autouse=True)
def clean_up_test_plots_directory():
    yield
    if os.path.exists(config.path_of_test_plots):
        shutil.rmtree(config.path_of_test_plots)


def test_save_to_html():
    # Test saving to HTML file without overwriting
    file_name = 'test_save_to_html'
    plot_multiple_figures(figures, name=file_name, show=False, path_of_plot=config.path_of_test_plots)

    # Check if the file exists
    assert os.path.exists(os.path.join(config.path_of_test_plots, file_name))


def test_show_in_browser():
    # Test showing the plots in the browser
    plot_multiple_figures(figures, show=True)

    # Note: We cannot directly check if the plots were shown in the browser programmatically.
    # This test case only checks that no exceptions were raised.


def test_unique_file_names():
    # Test unique file names with timestamps
    file_name = 'test_unique_file_names'
    plot_multiple_figures(figures, name=file_name, show=False)

    # Check if the file exists with the timestamp appended
    file_exists = any(file_name in f for f in os.listdir(config.path_of_test_plots))
    assert file_exists
