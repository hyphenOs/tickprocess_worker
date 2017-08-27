"""
Implementation of Worker for tick-process application
"""
from __future__ import print_function

import pandas as pd
import cProfile, pstats
import StringIO

from tickerplot.sql.sqlalchemy_wrapper import create_or_get_all_scrips_table
from tickerplot.sql.sqlalchemy_wrapper import create_or_get_nse_equities_hist_data
from tickerplot.sql.sqlalchemy_wrapper import get_metadata
from tickerplot.sql.sqlalchemy_wrapper import select_expr
from tickerplot.sql.sqlalchemy_wrapper import execute_one

from tickerplot.utils.logger import get_logger

class TickProcessWorkerExceptionDBInit(Exception):
    pass

class TickProcessWorkerExceptionInvalidDB(Exception):
    pass

class TickProcessProfiler(object):

    def __init__(self, parent=None, enabled=False, contextstr=None):
        self.parent = parent
        self.enabled = enabled

        self.stream = StringIO.StringIO()
        self.contextstr = contextstr or str(self.__class__)

        self.profiler = cProfile.Profile()

    def __enter__(self, *args):

        if not self.enabled:
            return None

        # Start profiling.
        self.stream.write("profile: {}: enter\n".format(self.contextstr))
        self.profiler.enable()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        if not self.enabled:
            return

        self.profiler.disable()

        sort_by = 'cumulative'
        ps = pstats.Stats(self.profiler, stream=self.stream).sort_stats(sort_by)
        ps.print_stats(0.1)

        self.stream.write("profile: {}: exit\n".format(self.contextstr))
        print (self.stream.getvalue())


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

    def aggregate(self, panel, period=None):


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
            for item in panel:
                agg_dict[item] = panel[item].resample(period,
                                                    how=resample_dict)
        return agg_dict

    def create_panels(self):
        """
        Create all panels.
        """
        try:
            with TickProcessProfiler(parent=self, enabled=self.enable_profiling) as p:
                scripdata_dict =self._do_read_db()
                self.panels['stocks_daily'] = pd.Panel(scripdata_dict)
            #self.apply_bonus_split_changes()

            #self.panels['stocks_monthly'] = self.foo()
            weekly_agg = self.aggregate(panel=self.panels['stocks_daily'],
                                            period='1W')
            self.panels['stocks_weekly'] = pd.Panel(weekly_agg)

            monthly_agg = self.aggregate(self.panels['stocks_daily'], '1M')
            self.panels['stocks_monthly'] = monthly_agg

            del weekly_agg, monthly_agg

        except Exception as e:
            self.logger.error(e)
            pass

if __name__ == '__main__':

    t = TickProcessWorker(db_path=
            'sqlite:////home/gabhijit/backup/personal-code/equities-data-utils/nse_hist_data.sqlite3')
    t.create_panels()
