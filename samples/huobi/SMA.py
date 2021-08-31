from ccxtbt import CCXTStore
from backtrader import Order
import backtrader as bt
from datetime import datetime, timedelta
import json


class TestStrategy(bt.Strategy):

    def __init__(self):
        self.sma = bt.indicators.SMA(self.data, period=21)
        self.bought = False
        
    def next(self):
        # Get cash and balance
        # New broker method that will let you get the cash and balance for
        # any wallet. It also means we can disable the getcash() and getvalue()
        # rest calls before and after next which slows things down.

        # NOTE: If you try to get the wallet balance from a wallet you have
        # never funded, a KeyError will be raised! Change LTC below as approriate
        if self.live_data:
            balance = self.broker.get_wallet_balance(['BTC','ETH','USDT'])
            cash = balance['USDT']['cash']
            if self.live_data and not self.bought:
                # Buy
                # size x price should be >10 USDT at a minimum at Binance
                # make sure you use a price that is below the market price if you don't want to actually buy
                #self.order = self.sell(size=0.002, exectype=Order.Limit, price=3200)
                #self.order = self.sell(size=0.002, exectype=Order.Market)
                #self.order = self.buy(size=0.005, exectype=Order.Limit, price=1500)
                self.order = self.sell(size=0.001, exectype=Order.Limit, price=6000)
                # And immediately cancel the buy order
                self.cancel(self.order)
                #self.cancel(self.order)
                self.bought = True
        else:
            # Avoid checking the balance during a backfill. Otherwise, it will
            # Slow things down.
            cash = 'NA'

        for data in self.datas:
            print('{} - {} | Cash {} | O: {} H: {} L: {} C: {} V:{} SMA:{}'.format(data.datetime.datetime(),
                                                                                   data._name, cash, data.open[0], data.high[0], data.low[0], data.close[0], data.volume[0],
                                                                                   self.sma[0]))

    def notify_data(self, data, status, *args, **kwargs):
        dn = data._name
        dt = datetime.now()
        msg= 'Data Status: {}'.format(data._getstatusname(status))
        print(dt, dn, msg)
        if data._getstatusname(status) == 'LIVE':
            self.live_data = True
        else:
            self.live_data = False

    def notify_order(self, order):
        if order.status in [order.Completed, order.Cancelled, order.Rejected]:
            self.order = None
        print('-' * 50, 'ORDER BEGIN', datetime.now())
        print(order)
        print('-' * 50, 'ORDER END')

    def notify_trade(self, trade):
        print('-' * 50, 'TRADE BEGIN', datetime.now())
        print(trade)
        print('-' * 50, 'TRADE END')            


with open('./params.json', 'r') as f:
    params = json.load(f)

cerebro = bt.Cerebro(quicknotify=True)

# Add the strategy
cerebro.addstrategy(TestStrategy)

# Create our store
config = {'apiKey': params["huobi"]["apikey"],
          'secret': params["huobi"]["secret"],
          'enableRateLimit': True,}

# IMPORTANT NOTE - Kraken (and some other exchanges) will not return any values
# for get cash or value if You have never held any BNB coins in your account.
# So switch BNB to a coin you have funded previously if you get errors
store = CCXTStore(exchange='huobipro', currency='ETH', config=config, retries=5, debug=False)

# Get the broker and pass any kwargs if needed.
broker = store.getbroker()
cerebro.setbroker(broker)

# Get our data
# Drop newest will prevent us from loading partial data from incomplete candles
hist_start_date = datetime.utcnow() - timedelta(minutes=1440)
data = store.getdata(dataname='ETH/USDT', name="ETHUSDT",
                     timeframe=bt.TimeFrame.Minutes, fromdate=hist_start_date,
                     compression=1, ohlcv_limit=1000, drop_newest=True) #, historical=True)

# Add the feed
cerebro.adddata(data)

# Run the strategy
cerebro.run()