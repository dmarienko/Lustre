import numpy as np
import pandas as pd
from glob import glob
from os.path import split, join
from tqdm.notebook import tqdm
import pytz, time, datetime
import sqlite3
from dataclasses import dataclass
from typing import Dict

from tools.utils.utils import mstruct, green, red, yellow, time_delta_to_str
from tools.analysis.timeseries import infer_series_frequency
from tools.analysis.tools import ohlc_resample


@dataclass
class TickData:
    instrument: str
    symbol: str
    exchange: str
    ticks: Dict[str, pd.DataFrame]
        
    def ohlc(self, timeframe):
        return ohlc_resample(self.ticks[self.symbol], timeframe)
    
    def ohlcs(self, timeframe):
        return ohlc_resample(self.ticks, timeframe)
    
    def datas(self, what):
        if what == 'ticks':
            return self.ticks
        return self.ohlcs(what) 
    
    def data(self, what):
        if what == 'ticks':
            return self.ticks[self.symbol]
        return self.ohlc(what) 
    

def __get_database_path(vendor, timeframe, path='./'):
    return join(path, f'{vendor}_{timeframe.upper()}.db')


def update_database(vendor, symbol, data, path='../data/'):
    timeframe = time_delta_to_str(pd.Timedelta(infer_series_frequency(data[:200])))
    tD = pd.Timedelta(timeframe)
    
    # Push data to sqlite3 db
    with sqlite3.connect(__get_database_path(vendor, timeframe, path)) as db:
        m_table = symbol
        table_exists = db.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{m_table}'").fetchone() is not None
        if table_exists:
            ranges = pd.read_sql_query(f"SELECT min(time) as Start, max(time) as End FROM {m_table}", db)
            last_time = ranges.End[0]
        else:
            last_time = data.index[0] - tD
        
        print(f' >> Inserting {green(symbol)} {yellow(timeframe)} for [{red(last_time)} -> {red(data.index[-1])}] ... ', end='')
        data_to_insert = data[pd.Timestamp(last_time) + tD:]
        data_to_insert.to_sql(m_table, db, if_exists='append', index_label='time')
        db.commit()
        print(yellow('[OK]'))
        
        
def ls_symbols(vendor, timeframe='1Min', path='../data'):
    with sqlite3.connect(__get_database_path(vendor, timeframe, path)) as db:
        tables = db.execute(f"SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        for t in tables:
            ranges = pd.read_sql_query(f"SELECT min(time) as Start, max(time) as End FROM {t[0]}", db)
            start_time = ranges.Start[0]
            last_time = ranges.End[0]
            print(f"{yellow(t[0])}:\t{red(start_time)} - {red(last_time)}")
    
        
def load_data(instrument, start='2017-01-01', end='2200-01-01', timeframe='1Min', path='../data'):
    if ':' not in instrument:
        raise ValueError("Wrong instrument name format, must be 'exchange:symbol' ")
    
    vendor, symbol = instrument.split(':')
    
    with sqlite3.connect(__get_database_path(vendor, timeframe, path)) as db:
        data = pd.read_sql_query(f"SELECT * FROM {symbol.upper()} where time >= '{start}' and time <= '{end}'", db, index_col='time')
        
    data.index = pd.DatetimeIndex(data.index)
    return TickData(instrument, symbol, vendor, {symbol: data})
        

def import_mt5_ohlc_data(vendor):
    for fn in glob(f'../data/{vendor}/*.csv.gz'):
        symbol = split(fn)[-1].split('_')[0].upper()
        rd = pd.read_csv(fn, sep='\t', parse_dates=[['<DATE>','<TIME>']])
        rd = rd.rename(columns=lambda x: x.strip('<>').lower()).rename(columns={'date>_<time':'time', 'tickvol':'volume'}).set_index('time')
        rd = rd.drop(columns='vol')
        update_database(vendor, symbol, rd)