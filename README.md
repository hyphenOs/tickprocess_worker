This repository contains code for `tickprocess` worker code. This is the backend for the worker code.

Each worker performs is implemented as follows -

1. Reads the data from the database and creates following Panels
   - NSE historic stocks data, daily
   - NSE historic stocks data, weekly
   - NSE historic stocks data, monthly
   - NSE historic indices data, daily
   - NSE historic indices data, weekly
   - NSE historic indices data, monthly

2. Implements to read and apply Bonus and Split to data when the data is read.

3. Implements following functions -

   - 'filter' - Method will be based on following attributes.
     - 'column' - Any of the OHLVC and then we will be worried about only that column
     - 'op' - Supported is 'ema' and 'sma' only. We'll add support for more and more 'ops'
     - 'op_params' - Params consumed by Op (typically num periods)
     - 'op_criteria' - 'above', 'below'
     - 'lookback' - How many data points to look-back?
     - 'threshold' - in percentage - applies to criteria
     - 'timescale' - Weekly, Daily or Monthly

  - 'compare' - Method will support following attributes
     - 'compare_with' - Symbol to compare with
     - 'compare_with_type' - 'stock' or 'index'
     - 'compare_op' - 'outperform', 'underperform'
     - 'periods' - How many periods
     - 'timescale' - Weekly, Daily or Monthly

  - 'distribution' - Method will return
Some examples -

List of all stocks near 52 week high

 = filter(column='h', threshold=0.05, lookback=52, timeframe='weekly')


List of all stocks with 'interesting volume' on weekly -
 = filter(column='v', op='ema', op_params={'span':52}, criteria='times', threshold=2.0, timeframe='weekly')

List of all stocks below 200 EMA on daily
 = filter(column='c', op='ema', op_params={'span':200}, criteria='below', threshold=1.0, timeframe='daily')


