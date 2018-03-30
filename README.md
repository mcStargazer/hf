<p> <center> <h2> The Hundred Fold (HF) Project </h2> </center> </p>

Ceremoniously named after the 8th scroll in Og Mandino's book, "The Greatest Salesman in the World." The hope is that money may be multipled through thoughtful trading. This project was designed and coded by Matt Collier, on an Ubuntu 16.04 LTS machine, with a then-current standard MySQL installation, and a Python environment built up from the base [Miniconda](https://conda.io/miniconda.html) distro. This code is offered under the [Unlicense](https://choosealicense.com/licenses/unlicense/). It is a work in progress. A brief description of the code follows. Also, there are many comments offered throughout the code and for most functions.

**common.py**
The script contains some useful definitions. Some functions are now unused as more sophisticated versions were developed or became uneeded. For instance get_dotw() is the most generalized date getting function and may replace some of the previous, more specific, date getting functions. Also, know that the get_config() function parses out credentials for database signon. I typically keep the config file in a privileged subdirectory.

**get_tickers.py**
First script to run each business day before the other scripts. This script's action is to look up the current list of S&P500 companies as recorded on wikipedia. It then adds any new companies to the 'symbols' database table, and deactivates companies that have fallen off the list. The purpose is to maintain a well defined population of equities to scan and choose from for potential trades.

**get_timeseries.py**
Second script to run each business day after about 9:15pm Eastern Time. Gathers End-Of-Day numbers for each of the S&P500 companies through the Quandl API. I didn't like the way the Quandl weekly summarization was working when I tested it, so I wrote my own weekly aggregation function. Data is inserted into the 'daily_data' and 'weekly_data' database tables.

**hundredfold.sql**
Some notes and SQL for installing the mysql database on a Linux machine. Also, some potentially useful maintenance SQL.

**make_metrics.py**
Third script to run each business day. Functions and metrics as interpreted by me from "The New Trading for a Living" (2014) by A. Elder. They include more or less: Exponential Moving Average, Force Index, Average True Range, Impulse, MACD-fast, MACD-slow, MACD-H, Stock Price Above (or, Below) EMA, and stock price Advance/Decline.

**requirements.txt**
A list of Python modules imported in these scripts.

**scan_db.py**
Fourth script to run each business day. This final script checks broad indicators (across the S&P500), sector indicators, and find potential longs and shorts. A webpage with subpages is generated for the day it's run. This date may be changed on the command line so that one could in principle run it for a past date, and step forward in time running it for subsequent dates to see how your predictions and paper trades work out. There is a known bug in this one for which I should some day upload a fix.

