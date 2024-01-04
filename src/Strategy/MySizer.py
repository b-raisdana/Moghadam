import backtrader as bt

from Config import config


class MySizer(bt.Sizer):
    def _getsizing(self, comminfo, cash, data, isbuy):
        """
                Custom sizer function to allocate order sizes.

                Parameters:
                  - cash: The current cash available in the portfolio.
                  - risk_percent: The percentage of cash to risk per trade (default is 10%).

                Returns:
                  - size: The order size to allocate.
                """
        # todo: test
        assert self.initial_cash is not None and self.initial_cash > 0
        risk_amount = self.initial_cash * config.order_max_capital_risk_precentage
        remaining_cash = self.initial_cash - self.broker.getvalue()

        if remaining_cash >= risk_amount:
            # Allocate 1/100 of the initial cash
            size = self.initial_cash * config.order_per_order_fixed_base_risk_percentage
        else:
            # If remaining cash is less than 10% of initial allocated cash, allocate zero
            size = 0

        return size
