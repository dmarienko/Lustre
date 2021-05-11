import pandas as pd
import numpy as np
from ira.series.Indicators import ATR, MovingMinMax
from ira.simulator.SignalTester import Tracker
from qlearn.tracking.trackers import ATRTracker, TakeStopTracker
from tools.analysis.timeseries import atr
from tools.analysis.tools import srows, scols
from tools.utils.utils import mstruct


def rad_indicator(x, period, mult):
    """
    RAD chandelier indicator (just for charting)
    """
    a = atr(x, period, smoother='ema')

    hh = x.high.rolling(window=period).max()
    ll = x.low.rolling(window=period).min()

    rad_long = hh - a * mult
    rad_short = ll + a * mult

    brk_d = x[(x.close.shift(1) > rad_long.shift(1)) & (x.close < rad_long)].index
    brk_u = x[(x.close.shift(1) < rad_short.shift(1)) & (x.close > rad_short)].index

    sw = pd.Series(np.nan, x.index)
    sw.loc[brk_d] = +1
    sw.loc[brk_u] = -1
    sw = sw.ffill()

    radU = rad_short[sw[sw > 0].index]
    radD = rad_long[sw[sw < 0].index]
    rad = srows(radU, radD)
    
    return mstruct(rad=rad, long=rad_long, short=rad_short, U=radU, D=radD)


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

    def get_stops(self):
        av = self.atr[1]
        m = self.mm[1]
        
        if av is None or m is None:
            # skip if ATR/hilo is not calculated yet
            return None, None
        
        ll, hh = m
        
        if not np.isfinite(av) or not np.isfinite(ll) or not np.isfinite(hh):
            # skip if ATR/hilo is not calculated yet
            return None, None

        l_stop = hh - self.stop_risk_mx * av
        s_stop = ll + self.stop_risk_mx * av
        
        return s_stop, l_stop

    def on_quote(self, quote_time, bid, ask, bid_size, ask_size, **kwargs):
        s_stop, l_stop = self.get_stops()
        qty = self._position.quantity

        if qty != 0:
            # calculat new levels
            s_stop, l_stop = self.get_stops()

            # check if we should pullup/down
            if qty > 0 and l_stop > self.stop:
                self.stop_at(quote_time, l_stop)
                if self.debug:
                    print(f'[{quote_time}] {self._instrument} pull up stop to {l_stop}')

            if qty < 0 and s_stop < self.stop:
                self.stop_at(quote_time, s_stop)
                if self.debug:
                    print(f'[{quote_time}] {self._instrument} pull down stop to {s_stop}')
                
        super().on_quote(quote_time, bid, ask, bid_size, ask_size, **kwargs)

    def on_signal(self, signal_time, signal_qty, quote_time, bid, ask, bid_size, ask_size):
        qty = self._position.quantity

        if qty != 0:
            return None

        s_stop, l_stop = self.get_stops()

        # skip signal if not ready
        if s_stop is None:
            return None

        if signal_qty > 0:
            if ask > l_stop:
                self.stop_at(signal_time, l_stop)
                if self.debug:
                    print(f'[{quote_time}] {self._instrument} entry long at ${ask} stop to {l_stop}')
            else:
                if self.debug:
                    print(f'[{quote_time}] {self._instrument} skip long : stop {l_stop} is above entry {ask}')
                signal_qty = np.nan

        elif signal_qty < 0:
            if bid < s_stop:
                self.stop_at(signal_time, s_stop)
                if self.debug:
                    print(f'[{quote_time}] {self._instrument} entry short at ${bid} stop to {s_stop}')
            else:
                if self.debug:
                    print(f'[{quote_time}] {self._instrument} skip short : stop {s_stop} is below entry {bid}')
                signal_qty = np.nan

        # call super method
        return signal_qty * self.position_size
