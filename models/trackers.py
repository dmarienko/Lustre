import pandas as pd
import numpy as np
from ira.series.Indicators import ATR, MovingMinMax
from ira.simulator.SignalTester import Tracker
from qlearn.tracking.trackers import ATRTracker, TakeStopTracker
from tools.analysis.timeseries import atr
from tools.analysis.tools import srows, scols
from tools.utils.utils import mstruct


class RADChandelier(TakeStopTracker):
    """
    RAD chandelier position tracker (no pyramiding only trailing stop)
    """
    def __init__(self, size, timeframe, period, stop_risk_mx, atr_smoother='sma', debug=False):
        super().__init__(debug)
        self.timeframe = timeframe
        self.period = period
        self.position_size = size
        self.stop_risk_mx = abs(stop_risk_mx)
        self.atr_smoother = atr_smoother

    def initialize(self):
        self.atr = ATR(self.period, self.atr_smoother)
        self.mm = MovingMinMax(self.period)
        self.ohlc = self.get_ohlc_series(self.timeframe)
        self.ohlc.attach(self.atr)
        self.ohlc.attach(self.mm)
        
        # current stop level
        self.level = None
        self.side = 0 # +1: up trend, -1: down trend
        self._dbg_values = {}
        
    def statistics(self):
        if self._dbg_values:
            from ira.utils.nb_functions import z_save
            z_save('RAD_test', self._dbg_values)
        return super().statistics()

    def get_stops(self):
        return self._stops(1)
    
    def _stops(self, n):
        av, m = self.atr[n], self.mm[n]
        if av is None or m is None:
            return None, None
        ll, hh = m
        if not np.isfinite(av) or not np.isfinite(ll) or not np.isfinite(hh):
            return None, None
        l_stop = hh - self.stop_risk_mx * av
        s_stop = ll + self.stop_risk_mx * av
        return s_stop, l_stop
    
    def update_stop_level(self) -> bool:
        if not self.ohlc.is_new_bar:
            return False
        
        # new bar just started
        s2, l2 = self._stops(2)
        s1, l1 = self._stops(1)
        if s2 is None:
            return False
            
        c1 = self.ohlc[1].close
        c2 = self.ohlc[2].close
        
        if c2 > l2 and c1 < l1:
#             self.debug(f'BROKE LONG SUP {self.ohlc[0].time}')
            self.side = -1
            self.level = s1
            
        if c2 < s2 and c1 > s1:
#             self.debug(f'BROKE SHORT RES {self.ohlc[0].time}')
            self.side = +1
            self.level = l1
        
        if self.side > 0:
            self.level = max(self.level, l1)
            
        if self.side < 0:
            self.level = min(self.level, s1)
            

    def on_quote(self, quote_time, bid, ask, bid_size, ask_size, **kwargs):
        # refresh current stop level
        self.update_stop_level()
        
        if self.side == 0 or self.level is None:
            return None
        
#         s_stop, l_stop = self.get_stops()
        qty = self._position.quantity
        
        # debug
#         s_stop, l_stop = self.get_stops()
#         self._dbg_values[self.ohlc[0].time] = {'Side': self.side, 'Level': self.level}

        if qty != 0:
            # calculate new levels
#             s_stop, l_stop = self.get_stops()

            # check if we should pullup/down
#             if qty > 0 and l_stop > self.stop:
#                 self.stop_at(quote_time, l_stop)
#                 self.debug(f'[{quote_time}] {self._instrument} pull up stop to {l_stop}')
            if qty > 0 and self.level > self.stop:
                self.stop_at(quote_time, self.level)
                self.debug(f'[{quote_time}] {self._instrument} pull up stop to {self.level}')

#             if qty < 0 and s_stop < self.stop:
#                 self.stop_at(quote_time, s_stop)
#                 self.debug(f'[{quote_time}] {self._instrument} pull down stop to {s_stop}')
            if qty < 0 and self.level < self.stop:
                self.stop_at(quote_time, self.level)
                self.debug(f'[{quote_time}] {self._instrument} pull down stop to {self.level}')
                
        super().on_quote(quote_time, bid, ask, bid_size, ask_size, **kwargs)

    def on_signal(self, signal_time, signal_qty, quote_time, bid, ask, bid_size, ask_size):
        qty = self._position.quantity

        if qty != 0:
            return None

#         s_stop, l_stop = self.get_stops()

        # skip signal if not ready
#         if s_stop is None:
#             return None

        if self.side == 0 or self.level is None:
            self.debug(f'[{quote_time}] {self._instrument} skip entry indicators are not ready: {self.level} / {self.side}')
            return None

        if signal_qty > 0:
            if self.side > 0 and ask > self.level:
                self.stop_at(signal_time, self.level)
                self.debug(f'[{quote_time}] {self._instrument} entry long at ${ask} stop to {self.level}')
            else:
                self.debug(f'[{quote_time}] {self._instrument} skip long : stop {self.level} is above entry {ask}')
                signal_qty = np.nan

        elif signal_qty < 0:
            if self.side < 0 and bid < self.level:
                self.stop_at(signal_time, self.level)
                self.debug(f'[{quote_time}] {self._instrument} entry short at ${bid} stop to {self.level}')
            else:
                self.debug(f'[{quote_time}] {self._instrument} skip short : stop {self.level} is below entry {bid}')
                signal_qty = np.nan

        # call super method
        return signal_qty * self.position_size