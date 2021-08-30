#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import json
import copy
from datetime import datetime

from backtrader import BrokerBase, Order
from backtrader.position import Position
from backtrader.utils.py3 import queue, with_metaclass

from .ccxtstore import CCXTStore


class CCXTOrder(Order):
    def __init__(self, owner, data, ccxt_order):
        self.owner = owner
        self.data = data
        self.ccxt_order = ccxt_order
        self.executed_fills = []
        self.ordtype = self.Buy if ccxt_order['side'] == 'buy' else self.Sell
        self.size = float(ccxt_order['amount'])

        super(CCXTOrder, self).__init__()


class MetaCCXTBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaCCXTBroker, cls).__init__(name, bases, dct)
        CCXTStore.BrokerCls = cls


class CCXTBroker(with_metaclass(MetaCCXTBroker, BrokerBase)):
    '''Broker implementation for CCXT cryptocurrency trading library.
    This class maps the orders/positions from CCXT to the
    internal API of ``backtrader``.

    Broker mapping added as I noticed that there differences between the expected
    order_types and retuned status's from canceling an order

    Added a new mappings parameter to the script with defaults.

    Added a get_balance function. Manually check the account balance and update brokers
    self.cash and self.value. This helps alleviate rate limit issues.

    Added a new get_wallet_balance method. This will allow manual checking of the any coins
        The method will allow setting parameters. Useful for dealing with multiple assets

    Modified getcash() and getvalue():
        Backtrader will call getcash and getvalue before and after next, slowing things down
        with rest calls. As such, th

    The broker mapping should contain a new dict for order_types and mappings like below:

    broker_mapping = {
        'order_types': {
            bt.Order.Market: 'market',
            bt.Order.Limit: 'limit',
            bt.Order.Stop: 'stop-loss', #stop-loss for kraken, stop for bitmex
            bt.Order.StopLimit: 'stop limit'
        },
        'mappings':{
            'closed_order':{
                'key': 'status',
                'value':'closed'
                },
            'canceled_order':{
                'key': 'result',
                'value':1}
                }
        }

    Added new private_end_point method to allow using any private non-unified end point

    '''

    order_types = {Order.Market: 'market',
                   Order.Limit: 'limit',
                   Order.Stop: 'stop',  # stop-loss for kraken, stop for bitmex
                   Order.StopLimit: 'stop limit'}

    mappings = {
        'closed_order': {
            'key': 'status',
            'value': 'closed'
        },
        'canceled_order': {
            'key': 'status',
            'value': 'canceled'}
    }

    def __init__(self, broker_mapping=None, debug=False, **kwargs):
        super(CCXTBroker, self).__init__()

        if broker_mapping is not None:
            try:
                self.order_types = broker_mapping['order_types']
            except KeyError:  # Might not want to change the order types
                pass
            try:
                self.mappings = broker_mapping['mappings']
            except KeyError:  # might not want to change the mappings
                pass

        self.store = CCXTStore(**kwargs)

        self.currency = self.store.currency

        self.positions = collections.defaultdict(Position)

        self.debug = debug
        self.indent = 4  # For pretty printing dictionaries

        self.notifs = queue.Queue()  # holds orders which are notified

        self.open_orders = list()

        self.startingcash = self.store._cash
        self.startingvalue = self.store._value
        
        self._last_op_time = 0

    def get_balance(self):
        self.store.get_balance()
        self.cash = self.store._cash
        self.value = self.store._value
        return self.cash, self.value

    def get_wallet_balance(self, currencys, params={}):
        result = {}
        balance = self.store.get_wallet_balance(params=params)
        for currency in currencys:
            cash = balance['free'][currency] if balance['free'][currency] else 0
            value = balance['total'][currency] if balance['total'][currency] else 0
            result[currency] = {}
            result[currency]['cash'] = cash
            result[currency]['value'] = value
        return result

    def getcash(self):
        # Get cash seems to always be called before get value
        # Therefore it makes sense to add getbalance here.
        # return self.store.getcash(self.currency)
        self.cash = self.store._cash
        return self.cash

    def getvalue(self, datas=None):
        # return self.store.getvalue(self.currency)
        self.value = self.store._value
        return self.value

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifs.put(order)

    def getposition(self, data, clone=True):
        # return self.o.getposition(data._dataname, clone=clone)
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos

    def next(self):
        if self.debug:
            print('Broker next() called')
        #===========================================
        # 每隔3秒操作一下
        nts = datetime.now().timestamp()
        if nts - self._last_op_time < 3:
            return
        self._last_op_time = nts
        #===========================================
        self._next()

    def _next(self):
        for o_order in list(self.open_orders):
            oID = o_order.ccxt_order['id']

            # Print debug before fetching so we know which order is giving an
            # issue if it crashes
            if self.debug:
                print('Fetching Order ID: {}'.format(oID))

            # Get the order
            ccxt_order = self.store.fetch_order(oID, o_order.data.p.dataname)

            # Check for new fills
            if 'trades' in ccxt_order and ccxt_order['trades'] is not None: #判断此订单是否有成交
                for fill in ccxt_order['trades']: #遍历此订单的所有成交
                    if fill not in o_order.executed_fills: #该成交是否被处理
                        fill_id, fill_dt, fill_size, fill_price = fill['id'], fill['datetime'], fill['amount'], fill['price']
                        o_order.executed_fills.append(fill_id) #记录该成交已经被处理
                        o_order.execute(fill_dt, fill_size, fill_price, 
                                        0, 0.0, 0.0, 
                                        0, 0.0, 0.0, 
                                        0.0, 0.0,
                                        0, 0.0) #处理该成交,内部会标注订单状态,部分成交还是完全成交
                        #准备通知上层策略
                        self.get_balance() #刷新账户余额
                        pos = self.getposition(o_order.data, clone=False) #获取对应仓位
                        pos.update(fill_size, fill_price) #刷新仓位
                        self.notify(o_order.clone()) #通知策略
            else:
                remaining, status = ccxt_order['remaining'], ccxt_order['status']
                fill_dt, fill_size, fill_price = ccxt_order['timestamp'], ccxt_order['filled'], ccxt_order['average']
                o_order.executed.dt = fill_dt
                o_order.executed.size = fill_size
                o_order.executed.price = fill_price
                o_order.executed.remsize = remaining
                #仓位计算比较麻烦,这里暂时不浪费时间做,需要再说,毕竟只有火币这样奇葩的平台才会这样
                if status == 'open':
                    self.get_balance() #刷新账户余额
                    o_order.partial()
                    self.notify(o_order.clone()) #通知策略
                elif status == 'closed':
                    self.get_balance() #刷新账户余额
                    o_order.completed()
                    self.notify(o_order.clone()) #通知策略

            if self.debug:
                print(json.dumps(ccxt_order, indent=self.indent))

            # Check if the order is closed
            if ccxt_order[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']:
                #如果该订单全部成交完成就是此状态,因为上面已经通知过策略,所以这里不再重复通知
                self.open_orders.remove(o_order)
            elif ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value']:
                #考虑两种情况:用户下了限价单没有成交,直接取消了,用户下了限价单部分成交,然后再取消
                o_order.cancel() #标注订单为取消状态
                self.notify(o_order.clone()) #通知策略
                self.open_orders.remove(o_order)

    def _submit(self, owner, data, exectype, side, amount, price, params):
        order_type = self.order_types.get(exectype) if exectype else 'market'
        created = int(data.datetime.datetime(0).timestamp()*1000)
        # Extract CCXT specific params if passed to the order
        params = params['params'] if 'params' in params else params
        params['created'] = created  # Add timestamp of order creation for backtesting
        ret_ord = self.store.create_order(symbol=data.p.dataname, order_type=order_type, side=side,
                                          amount=amount, price=price, params=params)
        _order = self.store.fetch_order(ret_ord['id'], data.p.dataname)
        order = CCXTOrder(owner, data, _order)
        order.price = ret_ord['price']
        self.open_orders.append(order)
        self.notify(order.clone())
        return order

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        return self._submit(owner, data, exectype, 'buy', size, price, kwargs)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        return self._submit(owner, data, exectype, 'sell', size, price, kwargs)

    def cancel(self, order):

        oID = order.ccxt_order['id']

        if self.debug:
            print('Broker cancel() called')
            print('Fetching Order ID: {}'.format(oID))

        # check first if the order has already been filled otherwise an error
        # might be raised if we try to cancel an order that is not open.
        ccxt_order = self.store.fetch_order(oID, order.data.p.dataname)

        if self.debug:
            print(json.dumps(ccxt_order, indent=self.indent))

        if ((ccxt_order[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']) or 
            (ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value'])):
            return order

        ccxt_order = self.store.cancel_order(oID, order.data.p.dataname)

        if self.debug:
            print(json.dumps(ccxt_order, indent=self.indent))
            print('Value Received: {}'.format(ccxt_order[self.mappings['canceled_order']['key']]))
            print('Value Expected: {}'.format(self.mappings['canceled_order']['value']))

        #统一在next函数中处理
        self._next()
        if ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value']:
            order.cancel()

        return order

    def get_orders_open(self, safe=False):
        return self.store.fetch_open_orders()

    def private_end_point(self, type, endpoint, params):
        '''
        Open method to allow calls to be made to any private end point.
        See here: https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods

        - type: String, 'Get', 'Post','Put' or 'Delete'.
        - endpoint = String containing the endpoint address eg. 'order/{id}/cancel'
        - Params: Dict: An implicit method takes a dictionary of parameters, sends
          the request to the exchange and returns an exchange-specific JSON
          result from the API as is, unparsed.

        To get a list of all available methods with an exchange instance,
        including implicit methods and unified methods you can simply do the
        following:

        print(dir(ccxt.hitbtc()))
        '''
        endpoint_str = endpoint.replace('/', '_')
        endpoint_str = endpoint_str.replace('{', '')
        endpoint_str = endpoint_str.replace('}', '')

        method_str = 'private_' + type.lower() + endpoint_str.lower()

        return self.store.private_end_point(type=type, endpoint=method_str, params=params)
