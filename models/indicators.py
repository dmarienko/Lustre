import pandas as pd
import numpy as np
from tools.analysis.timeseries import atr
from tools.analysis.tools import srows, scols
from tools.utils.utils import mstruct


def rad_indicator(x, period, mult, smoother='sma'):
    """
    RAD chandelier indicator (just for charting)
    """
    a = atr(x, period, smoother=smoother)

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
    
    # stop level
    mu, md = -np.inf, np.inf
    rs = {}
    for t, s in sw.items():
        if s < 0:
            mu = max(mu, rad_long.loc[t])
            rs[t] = mu 
            md = np.inf
        if s > 0:
            md = min(md, rad_short.loc[t])
            rs[t] = md
            mu = -np.inf
            
    rs = pd.Series(rs) 
    
    return mstruct(rad=rs, long=rad_long, short=rad_short, U=radU, D=radD)