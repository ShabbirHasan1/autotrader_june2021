import logging

import pandas as pd
import datetime

from ibapi.contract import Contract
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper

# types
from ibapi.common import *  # @UnusedWildImport
from ibapi.order import Order
from ibapi.order_state import OrderState

from finta import TA
from collections import deque

futures_contract = Contract()
futures_contract.symbol = 'RTY'
futures_contract.secType = 'FUT'
futures_contract.exchange = 'GLOBEX'
futures_contract.currency = 'USD'
futures_contract.lastTradeDateOrContractMonth = "202109"

REQ_ID_TICK_BY_TICK_DATE = 1
NUM_PERIODS = 9
ORDER_QUANTITY = 1
ticks_per_candle = 16000

# ! [socket_init]
class TestApp(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.globalCancelOnly = False
        self.simplePlaceOid = None
        self._my_errors = {}
        #self.contract = contract
        self.ticks_per_candle = ticks_per_candle
        self.nextValidOrderId = None
        self.started = False
        self.done = False
        self.position = 0
        # self.strategy = strategies.WMA(NUM_PERIODS, ticks_per_candle)
        self.last_signal = "NONE"
        self.pending_order = False
        self.tick_count = 0
        self.periods = NUM_PERIODS
        self.ticks = ticks_per_candle
        self.period_sum = self.periods * (self.periods + 1) // 2
        self.n = 0
        self.dq = deque()
        self.wma = 0
        self.signal = "NONE"
        self.high = 0
        self.max_value = 0
        self.min_value = 0
        self.atr_value = 0
        self.wma_target = 0
        self.target_up = 0
        self.target_down = 0
        self.dq1 = deque()
        self.i = 0

    @iswrapper
    # ! [connectack]
    def connectAck(self):
        if self.asynchronous:
            self.startApi()

    # ! [connectack]

    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
        # ! [nextvalidid]

        # we can start now
        self.start()

    def start(self):
        if self.started:
            return

        self.started = True

        if self.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.reqGlobalCancel()
        else:
            print("Executing requests")
            self.tickDataOperations_req()

            print("Executing requests ... finished")

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def tickDataOperations_req(self):
        self.reqTickByTickData(19002, futures_contract, "AllLast", 0, False)

    def calc_wma(self):
        data = list(self.dq)
        df = pd.DataFrame(data, columns=['close'])
        df['open'] = df['close']
        df['high'] = df['close']
        df['low'] = df['close']
        df['sma'] = TA.WMA(df, self.periods)
        self.wma = df['sma'].iloc[-1]
        self.dq.popleft()

    def calc_wma_clean(self):
        weight = 1
        wma_total = 0
        for price in self.dq:
            wma_total += price * weight
            weight += 1
        self.wma = wma_total / self.period_sum

    def update_signal(self, price: float):
        self.dq.append(price)
        self.n += 1
        if self.n < self.periods:
            return
        prev_wma = self.wma
        self.calc_wma()

        if prev_wma != 0:
            if self.wma > prev_wma:
                diff = self.wma - prev_wma
                self.wma_target = self.wma + diff

            elif self.wma < prev_wma:
                diff = prev_wma - self.wma
                self.wma_target = self.wma - diff

        if prev_wma != 0:
            if self.wma > prev_wma:
                self.signal = "LONG"
            elif self.wma < prev_wma:
                self.signal = "SHRT"

    def find_high(self, price: float):
        multiplier = 0.5
        self.dq1.append(price)
        self.max_value = max(self.dq1)
        self.min_value = min(self.dq1)
        self.atr_value = self.max_value - self.min_value
        self.target_up = self.wma_target + self.atr_value * multiplier
        self.target_down = self.wma_target - self.atr_value * multiplier
        self.i += 1
        if self.i > self.ticks:
            self.dq1.popleft()

    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float,
                          size: int, tickAttribLast: TickAttribLast, exchange: str,
                          specialConditions: str):
        print("TickByTickAllLast. ",
              "Candle:", str(self.tick_count // self.ticks_per_candle + 1).zfill(3),
              "Tick:", str(self.tick_count % self.ticks_per_candle + 1).zfill(3),
              "Time:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"),
              "Price:", "{:.2f}".format(price),
              "Size:", size,
              "Up Target", "{:.2f}".format(self.target_up),
              "Down Target", "{:.2f}".format(self.target_down),
              "WMA:", "{:.2f}".format(self.wma),
              "WMA_Target", "{:.2f}".format(self.wma_target),
              # "High", self.strategy.max_value,
              # "Low", self.strategy.min_value,
              "ATR", self.atr_value,
              # "Tick_List:", self.strategy.dq1,
              # "Current_List:", self.dq,
              self.signal)
        if self.tick_count % self.ticks_per_candle == self.ticks_per_candle - 1:
            self.update_signal(price)
            self.checkAndSendOrder()
        self.find_high(price)
        self.tick_count += 1

    @iswrapper
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        print("OrderStatus. ",
              "OrderId:", orderId,
              "Status:", status,
              "Filled:", filled,
              "Remaining:", remaining,
              "AvgFillPrice:", avgFillPrice,
              "PermId:", permId,
              "ParentId:", parentId,
              "LastFillPrice:", lastFillPrice,
              "ClientId:", clientId,
              "WhyHeld:", whyHeld,
              "MktCapPrice:", mktCapPrice)

    @iswrapper
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        print("OpenOrder. ",
              "OrderId:", orderId,
              "Contract:", contract,
              "Order:", order,
              "OrderState:", orderState)

    def checkAndSendOrder(self):
        print(f"Received {self.signal}")
        print(f"Last signal {self.last_signal}")

        if self.signal == "NONE" or self.signal == self.last_signal:
            print("Doing nothing")
            self.last_signal = self.signal
            return

        if self.signal == "LONG":
            self.sendOrder("BUY")
        elif self.signal == "SHRT" and self.last_signal != "NONE":
            self.sendOrder("SELL")
        else:
            print("Don't want to go naked short")

        self.last_signal = self.signal

    def sendOrder(self, action):
        order = Order()
        order.action = action
        order.totalQuantity = ORDER_QUANTITY
        order.orderType = "MKT"
        self.pending_order = True
        self.placeOrder(self.nextOrderId(), futures_contract, order)
        print(f"Sent a {order.action} order for {order.totalQuantity} shares")

def main():
    app = TestApp()
    try:
        # ! [connect]
        app.connect("127.0.0.1", port=7497, clientId=7)
        # ! [connect]
        print("serverVersion:%s connectionTime:%s" % (app.serverVersion(),
                                                      app.twsConnectionTime()))
        # ! [clientrun]
        app.run()
        # ! [clientrun]
    except:
        raise

if __name__ == "__main__":
    main()