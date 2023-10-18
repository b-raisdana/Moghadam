import pandera
from pandera import typing as pt

from Model.MultiTimeframeOHLC import OHLCV, MultiTimeframe


class PeaksValleys(OHLCV):
    peak_or_valley: pandera.typing.Series[str]
    strength: pandera.typing.Series[float] # pt.Timedelta  # pandera.typing.Series[np.timedelta64]


class MultiTimeframePeakValleys(PeaksValleys, MultiTimeframe):
    pass
