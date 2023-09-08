import pandera
from pandera import typing as pt

from DataPreparation import MultiTimeframe
from Model.MultiTimeframeOHLC import OHLC


class PeaksValleys(OHLC):
    peak_or_valley: pandera.typing.Series[str]
    strength: pt.Timedelta  # pandera.typing.Series[np.timedelta64]


class MultiTimeframePeakValleys(PeaksValleys, MultiTimeframe):
    pass
