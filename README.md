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

3. Implements two functions - `filter` and `distribution`. Details TBD.
