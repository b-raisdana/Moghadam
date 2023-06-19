import pandas as pd

from LevelDetectionConfig import config
from plotfig import plotfig


def plot_ohlc_with_peaks_n_valleys(ohlc, name, peaks, valleys):
    plotfig(ohlc, name=name, save=False, do_not_show=True) \
        .add_scatter(x=peaks.index.values, y=peaks['high'] + 1, mode="markers", name='Peak',
                     marker=dict(symbol="triangle-up", color="blue"),
                     hovertemplate="",
                     text=[
                         f"P:{peaks.loc[_x]['effective_time']}@{peaks.loc[_x]['high']}({pd.to_timedelta(peaks.loc[_x]['effective_time']) / pd.to_timedelta(config.times[0])})"
                         for _x in peaks.index.values]
                     ) \
        .add_scatter(x=valleys.index.values, y=valleys['low'] - 1, mode="markers", name='Valleys',
                     marker=dict(symbol="triangle-down", color="blue"),
                     hovertemplate="",
                     text=[
                         f"V:{valleys.loc[_x]['effective_time']}@{valleys.loc[_x]['low']}({pd.to_timedelta(valleys.loc[_x]['effective_time']) / pd.to_timedelta(config.times[0])})"
                         for _x in valleys.index.values]
                     ) \
        .update_layout(hovermode='x unified') \
        .show()
