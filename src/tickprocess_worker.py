"""
Implementation of Worker for tick-process application
"""
from __future__ import print_function

import time

import pandas as pd

from tickerplot.sql.sqlalchemy_wrapper import create_or_get_all_scrips_table
from tickerplot.sql.sqlalchemy_wrapper import create_or_get_nse_equities_hist_data
from tickerplot.sql.sqlalchemy_wrapper import get_metadata
from tickerplot.sql.sqlalchemy_wrapper import select_expr
from tickerplot.sql.sqlalchemy_wrapper import execute_one
from tickerplot.utils.profiler import TickerplotProfiler

from tickerplot.utils.logger import get_logger

class TickProcessWorkerExceptionDBInit(Exception):
    pass

class TickProcessWorkerExceptionInvalidDB(Exception):
    pass

class TickProcessWorker:

    def __init__(self, db_path=None, log_file=None):

        self.panels = {}
        self.symbols = []

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

        self.enable_profiling = False

    def set_profiling(self, enabled=False):
        self.enable_profiling = enabled

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

        hist_data = create_or_get_nse_equities_hist_data(metadata=self._db_meta)

        scripdata_dict = {}
        for scrip in self.symbols:
            sql_st = select_expr([hist_data.c.date,
                                hist_data.c.open, hist_data.c.high,
                                hist_data.c.low, hist_data.c.close,
                                hist_data.c.volume, hist_data.c.delivery]).\
                                    where(hist_data.c.symbol == scrip).\
                                            order_by(hist_data.c.date)

            scripdata = pd.io.sql.read_sql(sql_st, engine)

            scripdata.columns = ['date', 'open', 'high', 'low', 'close', 'volume',
                                'delivery']
            scripdata.reset_index(inplace=True)
            scripdata.set_index(pd.DatetimeIndex(scripdata['date']), inplace=True)
            scripdata.drop('date', axis=1, inplace=True)
            scripdata_dict[scrip] = scripdata

        return scripdata_dict

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
                agg_dict[item] = orig_dict[item].resample(period,
                                                    how=resample_dict)
        return agg_dict

    def create_panels(self):
        """
        Create all panels.
        """
        try:
            with TickerplotProfiler(parent=self, enabled=self.enable_profiling) as p:
                scripdata_dict = self._do_read_db()
                self.panels['stocks_daily'] = scripdata_dict
            #self.apply_bonus_split_changes()

            #self.panels['stocks_monthly'] = self.foo()
            #weekly_agg = self.aggregate(orig_dict=scripdata_dict, period='1W')
            #self.panels['stocks_weekly'] = pd.Panel(weekly_agg)

            #monthly_agg = self.aggregate(orig_dict=scripdata_dict, period='1M')
            #self.panels['stocks_monthly'] = monthly_agg

        except Exception as e:
            self.logger.error(e)
            pass

    def filter(self, **kw):
        scales_dict = { 'daily': 'stocks_daily',
                        'weekly' : 'stocks_weekly',
                        'monthly' : 'stocks_monthly' }
        functions = { 'ema' : pd.Series.ewm }
        if 'symbols' in kw:
            symlist = kw['symbols']

        if 'timescale' in kw:
            cur_dict = scales_dict[kw['timescale']]

        # Function to be applied on each 'col' of the 'symbols'
        # on the 'timescale'
        if 'function' in kw:
            function = functions[kw['function']]

        if 'col' in kw:
            col = kw['col']
        else:
            col = 'close'


        filtered = []
        not_filtered = []
        for symbol in symlist:
            res_series = function(cur_dict[symbol][col])


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

    t = TickProcessWorker(db_path=
            'sqlite:////home/gabhijit/backup/personal-code/equities-data-utils/nse_hist_data.sqlite3')
    t.create_panels()
    t.above_50_ema_daily()
