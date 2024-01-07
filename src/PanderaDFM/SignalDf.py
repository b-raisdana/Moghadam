# from __future__ import annotations
from datetime import datetime
from typing import Annotated

import backtrader as bt
import pandas as pd
import pandera
import pytz
from pandas import Timestamp
from pandera import typing as pt

from PanderaDFM.ExtendedDf import ExtendedDf, BasePanderaDFM


class SignalDFM(pandera.DataFrameModel):
    # from start of exact this candle the signal is valid
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # = pandera.Field(title='date')
    reference_date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    reference_timeframe: pt.Index[str]
    side: pt.Index[str]

    original_index: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    # from start of exact this candle the signal is in-valid
    end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    base_asset_amount: pt.Series[float] = pandera.Field(nullable=True, default=None)
    # the worst acceptable price for order execution.
    limit_price: pt.Series[float] = pandera.Field(nullable=True, default=None)
    stop_loss: pt.Series[float] = pandera.Field(nullable=True, default=None)  # , ignore_na=False
    take_profit: pt.Series[float] = pandera.Field(nullable=True, default=None)
    # the condition direction is reverse of limit direction.
    trigger_price: pt.Series[float] = pandera.Field(nullable=True, default=None)
    trigger_satisfied: pt.Series[bool] = pandera.Field(nullable=True, default=True)
    original_order_id: pt.Series[str] = pandera.Field(nullable=True, default=None)
    stop_loss_order_id: pt.Series[str] = pandera.Field(nullable=True, default=None)
    take_profit_order_id: pt.Series[str] = pandera.Field(nullable=True, default=None)
    order_is_active: pt.Series[bool] = pandera.Field(nullable=True, default=None)  # , ignore_na=False
    # todo: if the signal end changed we have to update signal orders
    updated: pt.Series[bool] = pandera.Field(nullable=True, default=True)

    class Config:
        # to resolve pandera.errors.SchemaError: column ['XXXX'] not in dataframe
        add_missing_columns = True
        # to resolve pandera.errors.SchemaError: expected series ['XXXX'/None] to have type datetime64[ns, UTC]
        # , got object
        coerce = True

    # @pandera.dataframe_check
    # def end_after_start_check(cls, df, *args, **kwargs):
    #     # Custom check to ensure 'end' is after 'date'
    #     return df['end'] > df['date']


class SignalDf(ExtendedDf):
    schema_data_frame_model = SignalDFM
    _sample_df = None
    _empty_obj = None

    @classmethod
    def new(cls, dictionary_of_data: dict = None, strict: bool = True) -> pt.DataFrame[SignalDFM]:
        result: pt.DataFrame[SignalDFM] = super().new(dictionary_of_data, strict)
        return result

    # @staticmethod
    # def took_profit(signal: pt.Series[SignalDFM], tick: BaseTickStructure) -> bool:
    #     if signal['side'] == 'buy':  # toddo: test
    #         if tick.high > signal['take_profit']:
    #             return True
    #     elif signal['side'] == 'sell':
    #         if tick.low < signal['take_profit']:
    #             return True
    #     else:
    #         raise Exception(f"Unexpected side({signal['side']}) in {signal} should be 'buy' or 'sell'.")
    #     return False

    # @staticmethod
    # def stopped(signal: pt.Series[SignalDFM], tick: BaseTickStructure) -> bool:
    #     if signal['side'] == 'buy':  # toddo: test
    #         if tick.low < signal['stop_loss']:
    #             return True
    #     elif signal['side'] == 'sell':
    #         if tick.high > signal['stop_loss']:
    #             return True
    #     else:
    #         raise Exception(f"Unexpected side({signal['side']}) in {signal} should be 'buy' or 'sell'.")
    #     return False

    @staticmethod
    def execution_type(signal):
        signal_dict = signal.to_dict()
        if pd.notna(signal_dict['stop_loss']) and pd.notna(signal_dict['limit_price']):
            # todo: this is not align with standard definition of StopLimit
            execution_type = bt.Order.StopLimit
        elif pd.notna(signal_dict['stop_loss']):
            execution_type = bt.Order.Stop  # todo: test
        elif pd.notna(signal_dict['limit_price']):
            execution_type = bt.Order.Limit  # todo: test
        else:
            execution_type = bt.Order.Market  # todo: test
        return execution_type

    # @staticmethod
    # def check_trigger(signal):
    #     assert self.trigger_price is not None # toddoo: test
    #     if self.isbuy():
    #         return self.parent.candle().high >= self.trigger_price
    #     elif self.issell():
    #         return self.parent.candle().high <= self.trigger_price
    #     return False

    @classmethod
    def to_str(cls, signal_index: datetime, signal: pt.Series[SignalDFM]):
        try:
            index = signal_index
            values = signal.to_dict()
            # effective_end = min( values['end'] if pd.notna(values['end']) else np.inf,  values['ttl'])
            result = (f"Signal@{pd.to_datetime(index).strftime('%y-%m-%d.%H-%M')}"
                      f"T{pd.to_datetime(values['end']).strftime('%d.%H-%M')}:"
                      f"{bt.Order.ExecTypes[SignalDf.execution_type(signal)]}"
                      f"{values['base_asset_amount']:.4f}@{values['limit_price']:.2f}SL{values['stop_loss']:.2f}"
                      f"TP{values['take_profit']:.2f}TR{values['trigger_price']:.2f}")
        except Exception as e:
            raise e
        return result


_sample_df = pd.DataFrame({
    'date': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'original_index': [
        Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'reference_date': [
        Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'reference_timeframe': ['1min'],
    'end': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=2).replace(tzinfo=pytz.UTC))],
    'side': ['buy'],
})

SignalDf._sample_df = _sample_df.set_index(['date', 'reference_date', 'reference_timeframe', 'side'])

a = SignalDf.new(
{
    'date': Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC)),
    'original_index':
        Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC)),
    'reference_date':
        Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC)),
    'reference_timeframe': '1min',
    'end': Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=2).replace(tzinfo=pytz.UTC)),
    'side': 'buy',
}
)