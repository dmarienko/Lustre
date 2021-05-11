from collections import defaultdict
import numpy as np
import pandas as pd

from tools.analysis.tools import srows, scols
from tools.utils.utils import mstruct

import matplotlib.pyplot as plt
import seaborn as sns


def plot_entries(execs, period=None):
    if period is not None:
        period = period if isinstance(period, slice) else slice(*period)
        execs = execs[period]

    for e in execs.iterrows():
        t, p, q, c = e[0], e[1].exec_price, e[1].quantity, e[1].comment

        if 'take' in c:
            plt.plot(t, p, 'o', c='g', ms=15)
            print(c)

        elif 'stop' in c:
            if 'short' in c:
                plt.plot(t, p, 'x', c='#6090f0', ms=12)
                plt.annotate(f'[{c}]', (t, p), xytext=(10,10), textcoords='offset points', size=12, c='w')
            elif 'long' in c:
                plt.plot(t, p, 'x', c='#fff000', ms=12)
                plt.annotate(f'[{c}]', (t, p), xytext=(-10,10), textcoords='offset points', size=12, c='w')
        else:
            _clr = 'w' if q > 0 else '#6090f0'
            plt.plot(t, p, '^' if q > 0 else 'v', c = _clr)
            plt.annotate(f'{p}', (t, p), xytext=(5,5), textcoords='offset points', c=_clr, size=12)
