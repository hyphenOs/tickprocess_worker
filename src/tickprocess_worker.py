#!venv/bin/python

#pylint: disable-msg:broad-except
#pylint: disable-msg=broad-except

"""
Implementation of Worker for tick-process application
"""
from __future__ import print_function

import time
from collections import namedtuple

import pandas as pd
from dask import delayed, compute

from tickerplot.sql.sqlalchemy_wrapper import create_or_get_all_scrips_table
from tickerplot.sql.sqlalchemy_wrapper import create_or_get_nse_equities_hist_data
from tickerplot.sql.sqlalchemy_wrapper import get_metadata
from tickerplot.sql.sqlalchemy_wrapper import select_expr
from tickerplot.sql.sqlalchemy_wrapper import execute_one
from tickerplot.utils.profiler import TickerplotProfiler

from tickerplot.utils.logger import get_logger

function_signature = namedtuple('Signature', ['name', 'obj_attr', 'is_callable'])

class TickProcessWorkerExceptionDBInit(Exception):
    pass

class TickProcessWorkerExceptionInvalidDB(Exception):
    pass

class TickProcessWorker:

    _supported_ops = { 'ema' : function_signature(*('ewm', 'mean', True)) }

    def __init__(self, db_path=None, log_file=None):

        self.panels = {}
        self.symbols = []
        self._last_symbols = None

        # Setup a logger, we might need it immediately below
        log_file = log_file or 'tickprocess_worker.log'
        self.logger = get_logger(name=str(self.__class__),
                                log_file=log_file)

        # DB related
        try:
            self._db_meta = get_metadata(db_path)
        except Exception as e:
            self.logger.error(e)
            self._db_meta = None
            raise TickProcessWorkerExceptionDBInit("Failed to Init DB")

        self._profiling = False

    @property
    def profiling(self):
        return self._profiling

    @profiling.setter
    def profiling(self, value):
        self._profiling = value

    @delayed
    def _get_data_for_symbol(self, symbol):
        """
        Internal function that reads DB for each symbol, we should see if we can make it parallel using Dask
        """
        if not self._db_meta:
            raise TickProcessWorkerExceptionInvalidDB("SQLAlchemy Metadata Not Initialized")

        engine = self._db_meta.bind
        if not engine:
            raise TickProcessWorkerExceptionInvalidDB("SQLAlchemy Metadata not bound to an Engine.")

        hist_data = create_or_get_nse_equities_hist_data(metadata=self._db_meta)

        sql_st = select_expr([hist_data.c.date,
                            hist_data.c.open, hist_data.c.high,
                            hist_data.c.low, hist_data.c.close,
                            hist_data.c.volume, hist_data.c.delivery]).\
                                where(hist_data.c.symbol == symbol).\
                                        order_by(hist_data.c.date)

        scripdata = pd.io.sql.read_sql(sql_st, engine)

        if not scripdata.empty:

            scripdata.columns = ['date', 'open', 'high', 'low', 'close', 'volume',
                                'delivery']
            scripdata.reset_index(inplace=True)
            scripdata.set_index(pd.DatetimeIndex(scripdata['date']), inplace=True)
            scripdata.drop('date', axis=1, inplace=True)

        return symbol, scripdata

    def _do_read_db(self):
        """
        Internal read_db function. From the database, populates the dictionary,
        that is used to create daily panel.
        """
        if not self._db_meta:
            raise TickProcessWorkerExceptionInvalidDB("SQLAlchemy Metadata Not Initialized")

        engine = self._db_meta.bind
        if not engine:
            raise TickProcessWorkerExceptionInvalidDB("SQLAlchemy Metadata not bound to an Engine.")

        self.symbols = self._do_read_symbols()

        values = [self._get_data_for_symbol(scrip) for scrip in self.symbols]

        results = compute(values, scheduler='threads')

        return dict((k, v) for k, v in results[0] if not v.empty)
        #return dict((k, v) for k, v in values if not v.empty)

    def _do_read_symbols(self):
        """
        Reads symbols from database.
        """
        if not self._db_meta.bind:
            raise TickProcessWorkerExceptionInvalidDB("SQLAlchemy Metadata not bound to an Engine.")
        all_scrips_table = create_or_get_all_scrips_table(
                                            metadata=self._db_meta)
        scrips_select_st = select_expr([all_scrips_table.c.nse_symbol]).\
                                   where(all_scrips_table.c.nse_traded == True)

        result = execute_one(scrips_select_st, engine=self._db_meta.bind)
        symbols = [row[0] for row in result.fetchall()]

        return symbols

    def aggregate(self, orig_dict, period=None):


        resample_dict = {'open' : 'first', 'high': 'max',
                'low' : 'min', 'close' : 'last',
                'volume' : 'sum', 'delivery' : 'sum'}

        agg_dict = {}

        period = period or ''
        if period in ('w', 'W', '1W', '1w'):
            period = '1W'
        elif period in ('m', 'M', '1M', '1m'):
            period = '1M'

        if period:
            for item in orig_dict:
                agg_dict[item] = orig_dict[item].resample(period).apply(resample_dict)
        return agg_dict

    def apply_bonus_split_changes(self):
        pass

    def create_panels(self):
        """
        Create all panels.
        """
        try:
            with TickerplotProfiler(parent=self, enabled=self._profiling) as profiler:
                print("before _do_read_db", time.time())
                scripdata_dict = self._do_read_db()
                print("after _do_read_db", time.time())
                self.panels['stocks_daily'] = scripdata_dict
            self.apply_bonus_split_changes()

            weekly_agg = self.aggregate(orig_dict=scripdata_dict, period='1W')
            self.panels['stocks_weekly'] = pd.Panel(weekly_agg)

            monthly_agg = self.aggregate(orig_dict=scripdata_dict, period='1M')
            self.panels['stocks_monthly'] = monthly_agg

            print(profiler.get_profile_data())

        except Exception as e:
            self.logger.exception(e)

    def filter(self, compute_column, check_column='close', symbols=None, op=None,
            op_params=None, op_criteria=None, lookback=None, threshold=None,
            timescale='daily'):
        """
        Applies a filter function based on certain criteria specified as
        arguments.

        """

        out_symbols = []
        scales_dict = { 'daily': 'stocks_daily',
                        'weekly' : 'stocks_weekly',
                        'monthly' : 'stocks_monthly' }

        if op is None and lookback is None:
            raise ValueError("One of the Op or Loopback must be specified.")

        if op not in self._supported_ops:
            raise ValueError("Op '{}' not in one of the supported Ops.".format(op))

        panel = self.panels.get(scales_dict.get(timescale, None), None)
        if not panel:
            raise ValueError("Dataframe for timescale {} not loaded."\
                    .format(timescale))

        if symbols is None:
            symbols = panel.keys()

        if op:
            function, attr, is_callable = self._supported_ops.get(op, None)

            if op_params is None or op_criteria is None:
                raise ValueError("Need to specify both 'op_params' and "
                    "'op_criteria' when Op is specified.")

            for symbol in symbols:
                last_val = panel[symbol][check_column].iloc[-1]

                series = panel[symbol][compute_column]
                f = getattr(series, function)
                result = f(**op_params)
                a = getattr(result, attr)
                if is_callable:
                    final = a()
                    last_computed = final.iloc[-1]
                else:
                    final = a
                    last_computed = final[-1]

                if op_criteria == 'above':
                    multiplier = 1 + threshold
                    if last_val >= (last_computed * multiplier):
                        out_symbols.append(symbol)
                elif op_criteria == 'below':
                    multiplier = 1 - threshold
                    if last_val <= (last_computed * multiplier):
                        out_symbols.append(symbol)
                elif op_criteria == 'times':
                    multiplier = threshold
                    if threshold < 1.0:
                        if last_val <= (last_computed * multiplier):
                            out_symbols.append(symbol)
                    if threshold > 1.0:
                        if last_val >= (last_computed * multiplier):
                            out_symbols.append(symbol)

        print(out_symbols)

    def above_50_ema_daily(self):

        daily_data = self.panels['stocks_daily']

        then = time.time()
        above_50 = []
        below_50 = []
        for symbol in daily_data:
            df = daily_data[symbol]
            if not df['close'].empty:
                df['ema_close_50'] = pd.ewma(df['close'], span=50)
                #if df['ema_close_50'][-1] <= df['close'][-1]:
                if df['ema_close_50'][-1].__le__(df['close'][-1]):
                    above_50.append(symbol)
                else:
                    below_50.append(symbol)
            else:
                pass #print (symbol)

        now = time.time()
        print (len(above_50), len(below_50), now - then)

if __name__ == '__main__':

    print(time.time())
    t = TickProcessWorker(db_path=
            'sqlite:////home/gabhijit/backup/personal-code/equities-data-utils/nse_hist_data.sqlite3')
    print(time.time())
    t.create_panels()
    print(time.time())
    t.profiling = False
    #t.above_50_ema_daily()
    with TickerplotProfiler(parent=t, enabled=t.profiling) as p:
        syms = t.filter(check_column='close', compute_column='close', op='ema', op_params={'span':50}, op_criteria='above', threshold=0.05)
        syms = t.filter(symbols=syms, check_column='close', compute_column='close', op='ema', op_params={'span':20}, op_criteria='above', threshold=0.05)
        syms = t.filter(check_column='volume', compute_column='volume', op='ema', op_params={'span':50}, op_criteria='times', threshold=2.00)
    print(p.get_profile_data())
    print(time.time())
