import pandas as pd
import numpy as np

from ira.series.Indicators import ATR, MovingMinMax
from ira.simulator.SignalTester import Tracker

import qlearn as q
from sklearn.base import BaseEstimator

from tools.analysis.timeseries import atr, smooth
from tools.analysis.tools import srows, scols, ohlc_resample
from tools.utils.utils import mstruct


@q.signal_generator
class Lustre(BaseEstimator):
    def __init__(self, timeframe, atr_period, mx, price_moving_period, vol_moving_period, tz='UTC'):
        self.timeframe = timeframe
        self.atr_period = atr_period
        self.mx = mx
        self.price_moving_period = price_moving_period
        self.vol_moving_period = vol_moving_period
        self.tz = tz
        
    def fit(self, x, y, **kwargs):
        return self
    
    def predict(self, x):
        xr = ohlc_resample(x[['open', 'high', 'low', 'close', 'volume']], self.timeframe, resample_tz=self.tz)
        
        # here we will use closes and volumes
        c, v = xr.close, xr.volume

        cs = smooth(c, 'ema', self.price_moving_period)
        vs = smooth(v, 'wma', self.vol_moving_period)
        a = atr(xr, self.atr_period, smoother='sma').shift(1)
        
        dc = c.diff()
        li = c[(dc > +a * self.mx) & (c > cs) & (v >= vs)].index  
        si = c[(dc < -a * self.mx) & (c < cs) & (v >= vs)].index  
        
        return q.shift_for_timeframe(srows(
            pd.Series(np.nan, xr.index[:1]), # first None signal to ignite tracker earlier
            pd.Series(+1, li), 
            pd.Series(-1, si)
        ), x, self.timeframe)