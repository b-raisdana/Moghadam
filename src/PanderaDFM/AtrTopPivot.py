from typing import Annotated

import pandas as pd
from pandera import typing as pt

from PanderaDFM.Pivot import MultiTimeframePivot


class MultiTimeAtrTopPivot(MultiTimeframePivot):
    movement_start: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    movement_end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    movement_start_value: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    movement_end_value: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
