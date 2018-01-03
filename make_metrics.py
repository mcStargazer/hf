#!/home/mcollier/miniconda3/bin/python
# -*- coding: utf-8 -*-

# Typical use cases:
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/make_metrics.py -v
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/make_metrics.py -s WMT
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/make_metrics.py -h


##############################################################################
# TBD:
#
# MESSAGE:
# index out of bounds
# TRACEBACK:
#   File "/home/mcollier/ONYX/W/portfolio/scripts/make_metrics.py", line 288, in <module>
#     metrics = make_metrics(con, span, ticker_id, today)
#
#   improve logging
#   A step ahead? Or, a step behind is better?
#   time optimization: indexing tables, pandas load, functionalization...
#   improve documentation
#
##############################################################################


# standard python library imports
import argparse
import datetime as dt
from itertools import islice
from os.path import join as join_paths
import sys
from time import time as t
import warnings

# third party python imports
import pandas as pd                              # conda install panda

# local imports
home = "/home/mcollier/ONYX/W/portfolio/scripts"
local_paths = [home, ]
sys.path.insert(0, [p for p in local_paths if p not in sys.path])
import common


##############################################################################
# defined user functions
##############################################################################

def calc_ema(smoothed, new_data, N=22):
    """
    INPUT:
        smoothed (Series) - Last record's smoothed EMA value (a copy)
        new_data (Series) - New values, prepended with last value
        N (int) - Number of time periods for smoothing
    OUTPUT:
        Exponential Moving Average as a pandas Series
    """

    K = 2/(N + 1)    # coefficient for incoming datum
    J = 1 - K        # coefficient for fading data

  # if first smoothed, use last data value
    if not len(smoothed.index):
        smoothed[new_data.index.values[0]] = new_data[0]

    for n,index in enumerate(new_data[1:].index, start=1):
        smoothed[index] = K*new_data[n] + J*smoothed[n-1]

    return smoothed


def calc_force(force, new_closes, new_volumes):
    """
    INPUTS:
        force (Series) - A copy of last record's force value.
        new_closes (Series) - New values, prepended with last closing price
        new_volumes (Series) - New values, prepended with last volume
    OUTPUT:
        Force index as a pandas Series.
    """

    for n,index in enumerate(new_closes[1:].index, start=1):
        force[index] = ( new_closes[n] - new_closes[n-1] )*new_volumes[n]

    return force


def calc_tr(tr, new_highs, new_lows, new_closes):
    """
    INPUTS:
        tr (Series) - A copy of last record's True Range value.
        new_highs (Series) - New values, prepended with last high price
        new_lows (Series) - New values, prepended with last low price
        new_closes (Series) - New values, prepended with last close price
    OUTPUT:
        True Range as a pandas Series
    """
    for n,index in enumerate(new_highs[1:].index, start=1):
        tr[index] = max(  [abs(new_highs[n] -   new_lows[n]  ),
                           abs(new_highs[n] - new_closes[n-1]),
                           abs( new_lows[n] - new_closes[n-1])]  )
    return tr


def calc_impulse(impulse, macdh, ema12):
    """
    INPUTS:
        impulse (Series) - Previous impulse value
        macdh (Series) - New values, prepended with last MACDH value.
        ema12 (Series) - New values, prepended with last EMA12 value.
    OUTPUT:
        Impulse as a pandas Series
    """

    # take first order differences
    ema12_diff = ema12 - ema12.shift(1)
    macdh_diff = macdh - macdh.shift(1)

    # drop the first elements (NaN)
    ema12_diff = ema12_diff.drop(ema12_diff.index[[0,]])
    macdh_diff = macdh_diff.drop(macdh_diff.index[[0,]])

    # get individual signs
    sign = lambda x: (1, -1)[x < 0] if x != 0 else 0
    ema12_diff = ema12_diff.apply(sign)
    macdh_diff = macdh_diff.apply(sign)

    # combine individual signs into single impulse encoding
    condense = lambda x: -1 if x == -2 else 1 if x == 2 else 0
    new = (ema12_diff + macdh_diff).apply(condense)

    return impulse.append(new)


def make_metrics(con, span, ticker_id, today):
    """
    INPUTS:
        con (mysql) - pymysql database connection
        span (str) - Specify "daily" or "weekly"
        ticker_id (int) - Unique symbol id from symbols table
        today (str) - ISO 8601 date string of form YYYY-MM-DD
    OUTPUT:
        All basic metrics returned in a pandas dataframe.
    """

    # get last record(s) from the time span's metrics table
    sql = ("SELECT * FROM {}_metrics WHERE symbol_id={}"
           " ORDER BY price_date DESC LIMIT 1;")
    metrics = pd.read_sql_query(sql.format(span, ticker_id),
                                con, index_col="price_date").drop('id', 1)
    if not len(metrics.index): price_date = "2000-01-03"
    else: price_date = metrics.index[0]

    # pull necessary data from the span's data table
    sql = """SELECT price_date, high, low, close, volume FROM {}_data
             WHERE symbol_id={} AND price_date BETWEEN "{}" AND "{}"
             ORDER BY price_date;"""
    data = pd.read_sql_query(sql.format(span, ticker_id, price_date, today),
                             con, index_col="price_date", coerce_float=True)
    #data = pd.DataFrame.from_csv("./data/wmt_daily_df.csv")
    #metrics = pd.DataFrame(columns=cols.split(", "))
    #pd.set_option('display.width', 180)
    #pd.set_option('display.max_rows', 1000)
    #data[["high","low","close","volume"]].head()
    #data[["high","low","close","volume"]].tail()

    # Exponential Moving Averages of the price data
    ema12 = calc_ema(metrics.ema12[:], data.close, N=12)
    ema26 = calc_ema(metrics.ema26[:], data.close, N=26)
    ema50 = calc_ema(metrics.ema50[:], data.close, N=50)

    # Force, and smoothed 2-day version
    force = calc_force(metrics.force[:], data.close, data.volume)
    force2 = calc_ema(metrics.force2[:], force, N=2)

    # Average True Range, and smoothed 13-day version
    tr = calc_tr(metrics.tr[:], data.high, data.low, data.close)
    atr13 = calc_ema(metrics.atr13[:], tr, N=13)

    # Moving Average Convergence Divergence family
    macdf = (ema12 - ema26).rename("macdf")             # MACD Fast Line
    macds = calc_ema(metrics.macds[:], macdf, N=9)      # MACD Slow (signal)
    macdh = (macdf - macds).rename("macdh")             # MACD Histogram

    # Impulse
    impulse = calc_impulse(metrics.impulse[:], macdh, ema12).rename("impulse")

    # stock price is above or below the ema (1 or -1, default of 0)
    sign = lambda x: (1, -1)[x < 0] if x != 0 else 0
    gt12 = ( data.close - ema12 ).apply(sign).rename("gt12")
    gt26 = ( data.close - ema26 ).apply(sign).rename("gt26")
    gt50 = ( data.close - ema50 ).apply(sign).rename("gt50")

    # stock price advances or declines (1 or -1, default of 0)
    ad = (data.close - data.close.shift(1)).apply(sign).rename("ad")

    # wrap up the results into a return dataframe
    metrics = pd.concat([ema12, ema26, ema50, force, force2, tr, atr13,
                      macdf, macds, macdh, impulse,
                      gt12, gt26, gt50, ad], axis=1)
    metrics.insert(0, "symbol_id", ticker_id)
    metrics['last_update'] = today
    metrics = metrics.where((pd.notnull(metrics)), None)

    return metrics


def data_replace(con, span, metrics):
    """
    INPUTS:
        con (mysql) - pymysql database connection
        span (str) - Specify "daily" or "weekly"
        metrics (list) - List of tuples of metrics
    OUTPUT:
        Data replacement with homebrew method. A discussion on mysql 'replace'
        is at code.openark.org/blog/mysql/replace-into-think-twice.
    """

    # pre-loop sql/data preparation
    table = "{}_metrics".format(span)
    cols = ["price_date", "symbol_id", "ema12", "ema26", "ema50",
            "force", "force2", "tr", "atr13", "macdf", "macds",
            "macdh", "impulse", "gt12", "gt26", "gt50", "ad", "updated"]
    cols = ["`{}`".format(s) for s in cols]
    header = ",".join(cols)
    esses = ("%s,"*len(cols))[:-1]
    insert = "INSERT INTO {} ({}) VALUES ({})".format(table, header, esses)

    with con.cursor() as cur:  # cur = con.cursor()

        # loop over rows, finish the sql and execute it
        for row in metrics:
            update = "UPDATE " + ",".join(["{}={}".format(r[0], r[1])
                                     for r in islice(zip(cols, row), 2, None)])
            sql = "{} ON DUPLICATE KEY {}".format(insert, update)
            sql = sql[:-10] + "'" + sql[-10:] + "';"
            results = cur.execute(sql.replace("None", "NULL"), row)

    return results


##############################################################################
# Treat this file as a script if invoked as __main__
##############################################################################

if __name__ == "__main__":

    # load configuration from commented JSON into dictionary
    conf = common.get_config("/etc/local/hf.conf")

    # assign parsed commandline values to working objects
    p = argparse.ArgumentParser()
    p.add_argument("-s", "--single", default="",
        help="pull a single ticker... i.e. -s WMT")
    p.add_argument("-v", "--verbose", action="store_true",
        help="print extra information on stdout")
    p.add_argument("-N", "--no_insert", action="store_true",
        help="suppress insertion of data into db tables if true")
    p.add_argument("-R", "--no_report", action="store_true",
        help="option to suppress reporting at end")
    args = p.parse_args()

    # set up functions and parameters
    vprint = print if args.verbose else lambda *a, **k: None
    vprint("Setting up environment...")
    wpack = ["WARNING(S):\n", 0]
    epack = ["ERROR(S):\n", 0]
    now = dt.datetime.today()
    today = now.date().isoformat()
    warnings.filterwarnings("ignore")  # ignore Decimal(19,4) truncation
    con = common.get_connection(conf)
    tuplefy = lambda df: [ tuple(["{}".format(i.isoformat()),
                                  *list(r.values)]) for i,r in df.iterrows()]
    lentickers, tickers = common.read_tickers(con, args.single)
    t_keys = list(tickers.keys())
    t_keys.sort()
    count, t00 = 0, t()

    # calculate metrics for each ticker symbol
    vprint("Calculating metrics for each ticker symbol...")
    for t_key in t_keys:

        ticker_id = tickers[t_key][0]        # t_key, ticker_id = "TPR", 507
        t0 = t()
        err = ''
        times = "({}/{}) {:>5s} Total: {:6.2f}"

        try:

            for span in ["daily",                          # span = "daily"
                         "weekly"]:                        # span = "weekly"
                t1 = t()
                metrics = make_metrics(con, span, ticker_id, today)
                metrics = tuplefy(metrics)
                if not args.no_insert:
                    data_replace(con, span, metrics)
                times += ", {}: {:6.2f}".format(span, t()-t1)

            con.commit()

        except Exception as err_msg:

            err = "ERROR on {} {}".format(span, t_key)
            epack = common.handle(err_msg, epack)
            con.rollback()

        finally:

            count += 1
            vprint(times.format(count, lentickers, t_key, t()-t0) + "  " + err)
            # first run took 11.6hrs


    ##########################################################################
    # reporting
    ##########################################################################

    if not args.no_report:

        vprint("Reporting...\n")
        msg = "OPTIONS:\n"
        msg += " single: {}\n verbose: {}\n no_insert: {}\n no_report: {}\n"
        msg = msg.format(args.single if args.single else False, args.verbose,
                         args.no_insert, args.no_report)
        msg += "TIME ELAPSED:\n"
        msg += " Processing all span-key pairs took: {:.2f}\n".format(t()-t00)
        if not epack[1]: msg += "ERRORS: None\n"
        else: msg += "{}\n".format(epack[0])

        f = open(home + "/logs/{}_metrics.log".format(today), "a")
        f.write("#"*79 + '\n')
        f.write(now.ctime() + '\n')
        f.write(msg)

    else:

        msg = "reporting turned off!\n"

    # finally
    vprint(msg)
    vprint(epack[0])
    con.close()

