import backtrader as bt


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
        else:  # self.issell()
            self.trigger_satisfied |= self.parent.data.close[0] <= self.trigger_price
        return self.trigger_satisfied
