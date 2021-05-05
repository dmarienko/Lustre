import os
import traceback
import pandas as pd
from collections import OrderedDict, namedtuple
from datetime import timedelta as timedelta_t
from typing import Union, List, Set, Dict


def __wrap_with_color(code):
    def inner(text, bold=False):
        c = code
        if bold:
            c = "1;%s" % c
        return "\033[%sm%s\033[0m" % (c, text)
    return inner


red, green, yellow, blue, magenta, cyan, white = (
    __wrap_with_color('31'),
    __wrap_with_color('32'),
    __wrap_with_color('33'),
    __wrap_with_color('34'),
    __wrap_with_color('35'),
    __wrap_with_color('36'),
    __wrap_with_color('37'),
)
            
    
def is_localhost(host):
    return host.lower() == 'localhost' or host == '127.0.0.1'


def time_delta_to_str(d: Union[int, timedelta_t, pd.Timedelta]):
    """
    Convert timedelta object to pretty print format

    :param d:
    :return:
    """
    seconds = d.total_seconds() if isinstance(d, (pd.Timedelta, timedelta_t)) else int(d)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    r = ''
    if days > 0:
        r += '%dD' % days
    if hours > 0:
        r += '%dH' % hours
    if minutes > 0:
        r += '%dMin' % minutes
    if seconds > 0:
        r += '%dS' % seconds
    return r
    

class mstruct:
    """
    Dynamic structure (similar to matlab's struct it allows to add new properties dynamically)

    >>> a = mstruct(x=1, y=2)
    >>> a.z = 'Hello'
    >>> print(a)

    mstruct(x=1, y=2, z='Hello')
    
    >>> mstruct(a=234, b=mstruct(c=222)).to_dict()
    
    {'a': 234, 'b': {'c': 222}}

    """

    def __init__(self, **kwargs):
        _odw = OrderedDict(**kwargs)
        self.__initialize(_odw.keys(), _odw.values())

    def __initialize(self, fields, values):
        self._fields = list(fields)
        self._meta = namedtuple('mstruct', ' '.join(fields))
        self._inst = self._meta(*values)

    def __getattr__(self, k):
        return getattr(self._inst, k)

    def __dir__(self):
        return self._fields

    def __repr__(self):
        return self._inst.__repr__()

    def __setattr__(self, k, v):
        if k not in ['_inst', '_meta', '_fields']:
            new_vals = {**self._inst._asdict(), **{k: v}}
            self.__initialize(new_vals.keys(), new_vals.values())
        else:
            super().__setattr__(k, v)

    def __getstate__(self):
        return self._inst._asdict()

    def __setstate__(self, state):
        self.__init__(**state)

    def __ms2d(self, m):
        r = {}
        for f in m._fields:
            v = m.__getattr__(f)
            r[f] = self.__ms2d(v) if isinstance(v, mstruct) else v
        return r
    
    def to_dict(self):
        """
        Return this structure as dictionary
        """
        return self.__ms2d(self)
    
    def copy(self):
        """
        Returns copy of this structure
        """
        return dict2struct(self.to_dict())

    
def dict2struct(d: dict):
    """
    Convert dictionary to structure
    >>> s = dict2struct({'f_1_0': 1, 'z': {'x': 1, 'y': 2}})
    >>> print(s.z.x)
    1
    
    """
    m = mstruct()
    for k, v in d.items():
        if isinstance(v, dict):
            v = dict2struct(v)
        m.__setattr__(k, v)
    return m
