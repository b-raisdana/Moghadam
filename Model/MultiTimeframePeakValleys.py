import pandera

from Model.MultiTimeframe import MultiTimeframe
from Model.MultiTimeframeOHLCV import OHLCV


class PeakValleys(OHLCV):
    peak_or_valley: pandera.typing.Series[str]
    strength: pandera.typing.Series[float]  # pt.Timedelta  # pandera.typing.Series[np.timedelta64]
    permanent_strength: pandera.typing.Series[bool]


class MultiTimeframePeakValleys(PeakValleys, MultiTimeframe):
    pass
