import backtrader as bt
import pandas as pd

from Config import config
from Model.Signal import Signal
from ohlcv import read_base_timeframe_ohlcv


class ExtendedOrder(bt.Order):
    trigger_satisfied: bool = False

    def __init__(self, parent, limit_price, stop_loss, take_profit, trigger_price, *args, **kwargs):
        self.trigger_price = trigger_price
        self.limit_price = limit_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        super().__init__(parent, *args, **kwargs)

    def check_trigger(self, price):
        if self.isbuy():
            return price >= self.trigger_price
        elif self.issell():
            return price <= self.trigger_price
        return False

    def __init__(self, parent, trigger_price, *args, **kwargs):
        self.trigger_price = trigger_price
        super().__init__(parent, *args, **kwargs)

    def execute(self):
        # Check trigger condition before executing
        if self.check_trigger():
            # Perform the actual order execution
            return super().execute()
        else:
            # Hold the order if trigger condition is not satisfied
            # self.parent.log("Trigger condition not satisfied. Order on hold.")
            return None

    def check_trigger(self):
        # Implement your trigger condition logic here
        # Return True if the condition is satisfied, False otherwise
        if self.isbuy():
            self.trigger_satisfied |= (self.parent.data.close[0] >= self.trigger_price)
        else: # self.issell()
            self.trigger_satisfied |= self.parent.data.close[0] <= self.trigger_price
        return self.trigger_satisfied


class BasePatternStrategy(bt.Strategy):
    signal_df: Signal

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def add_signal_source(self, signal_source: Signal):
        self.signal_df = signal_source
        Signal.cast_and_validate(signal_source)

    def __init__(self):
        self.dataclose = self.datas[0].close

    def next(self):
        """
        I have a df with schema of Signal, I want to check if current testing time is after a signal and the time is
        before signal end or the signal end is np.INF or NA, and the order related to the signal has not been placed
        yet, put the order according to the signal.
        :return:
        """
        if 'main_order_id' not in self.signal_df.columns:
            self.signal_df['main_order_id'] = pd.NA
        _datetime = self.datas[0].datetime.datetime(0)
        active_signals = self.signal_df[
            (self.signal_df['date'] <= _datetime) &
            ((self.signal_df['end'].isna()) | (self.signal_df['end'] > _datetime)) &
            (self.signal_df['main_order_id'].isna())
            ]
        if not active_signals.empty and not self.order_placed:
            assert \
                all(item in active_signals.columns for item in
                    ['side', 'base_asset_amount', 'limit_price', 'stop_loss', 'take_profit'])
            self.log('Close, %.2f' % self.dataclose[0])
            for start, signal in active_signals.iterrows():
                side = signal['side']
                base_asset_amount = signal['base_asset_amount']
                limit_price = signal['limit_price']
                stop_loss = signal['stop_loss']
                take_profit = signal['take_profit']
                trigger_price = signal['trigger_price']

                # Placeholder for actual order placement logic
                # Replace this with your order placement code
                if side == 'buy':
                    order_executor = self.buy
                    bracket_executor = self.sell_bracket
                elif side == 'sell':
                    order_executor = self.sell
                    bracket_executor = self.buy_bracket
                else:
                    raise Exception('Unknown side %s' % side)
                # Placeholder for setting stop loss and take profit
                if pd.notna(stop_loss) and pd.notna(limit_price):
                    execution_type = ExtendedOrder.StopLimit
                elif pd.notna(stop_loss):
                    execution_type = ExtendedOrder.Stop
                elif pd.notna(limit_price):
                    execution_type = ExtendedOrder.Limit
                else:
                    execution_type = ExtendedOrder.Market

                if pd.notna(take_profit):
                    main_order_id, _, _ = bracket_executor(size=base_asset_amount, exectype=execution_type, limitprice=take_profit,
                                     price=limit_price, stopprice=stop_loss, trigger_price=trigger_price)
                    # order_executor(
                    #     size=base_asset_amount, exectype=execution_type, price=take_profit, parent=bracket_executor(
                    #         limitprice=take_profit, stopprice=stop_loss
                    #     )
                    # )
                    self.signal_df.loc[start, 'main_order_id'] = main_order_id
                else:
                    main_order_id = order_executor(size=base_asset_amount, exectype=execution_type, price=stop_loss,
                                   limit_price=limit_price, trigger_price=trigger_price)
                    self.signal_df.loc[start, 'main_order_id'] = main_order_id


def test_strategy():
    cerebro = bt.Cerebro()
    cerebro.addstrategy(BasePatternStrategy)
    # config.processing_date_range = '23-12-25.00-00T23-12-26.23-59'
    raw_data = read_base_timeframe_ohlcv(config.processing_date_range)
    data = bt.feeds.PandasData(dataname=raw_data, datetime=None, open=0, close=1, high=2, low=3, volume=4,
                               openinterest=-1)
    # PandasData(dataname=raw_data)
    cerebro.adddata(data)
    cerebro.broker.setcash(100.0)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
