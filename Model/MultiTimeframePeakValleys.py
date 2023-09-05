import numpy as np
import pandera

from DataPreparation import MultiTimeframe
from Model.MultiTimeframeOHLC import OHLC


class PeaksValleys(OHLC):
    peak_or_valley: pandera.typing.Series[str]
    strength: pandera.typing.Series[np.timedelta64]


class MultiTimeframePeakValleys(PeaksValleys, MultiTimeframe):
    pass
