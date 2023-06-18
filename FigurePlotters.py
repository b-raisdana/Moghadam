import pandas as pd

from LevelDetection import aggregate_test_prices, _peaks, _valleys
from LevelDetectionConfig import config
from plotfig import plotfig


def plot_ohlc_with_peaks_n_valleys(ohlc, name, peaks, valleys)
    plotfig(aggregate_test_prices, name=name, save=False, do_not_show=True) \
        .add_scatter(x=_peaks.index.values, y=_peaks['high'] + 1, mode="markers", name='Peak',
                     marker=dict(symbol="triangle-up", color="blue"),
                     hovertemplate="",
                     text=[
                         f"P:{_peaks.loc[_x]['effective_time']}@{_peaks.loc[_x]['high']}({pd.to_timedelta(_peaks.loc[_x]['effective_time']) / pd.to_timedelta(config.times[0])})"
                         for _x in _peaks.index.values]
                     ) \
        .add_scatter(x=_valleys.index.values, y=_valleys['low'] - 1, mode="markers", name='Valleys',
                     marker=dict(symbol="triangle-down", color="blue"),
                     hovertemplate="",
                     text=[
                         f"V:{_valleys.loc[_x]['effective_time']}@{_valleys.loc[_x]['low']}({pd.to_timedelta(_valleys.loc[_x]['effective_time']) / pd.to_timedelta(config.times[0])})"
                         for _x in _valleys.index.values]
                     ) \
        .update_layout(hovermode='x unified') \
        .show()
