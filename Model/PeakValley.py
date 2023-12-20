import pandera

from Model.MultiTimeframe import MultiTimeframe
from Model.OHLCV import OHLCV


class PeakValley(OHLCV):
    peak_or_valley: pandera.typing.Series[str]
    strength: pandera.typing.Series[float]  # pt.Timedelta  # pandera.typing.Series[np.timedelta64]
    permanent_strength: pandera.typing.Series[bool]


class MultiTimeframePeakValley(PeakValley, MultiTimeframe):
    pass
