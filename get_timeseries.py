#!/home/mcollier/miniconda3/bin/python
# -*- coding: utf-8 -*-

# Typical use cases:
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/get_timeseries.py -vD
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/get_timeseries.py -N -s WMT
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/get_timeseries.py -h


##############################################################################
# TBD:
#
#   improve logging
#   !!!! CORRECT "ON DUPLICATE KEY UPDATE" behavior
#   change volumes to ints in DB?
#   drop adjusted values?
#   improve documentation
#   add code snippet to inactivate a ticker manually
#   develop adaptive-sleep-size function?
#
##############################################################################


# standard python library imports
import argparse
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_HALF_EVEN as RHE
import os
import sys
from time import sleep, time
import warnings

# third party python imports
from pandas import concat                        # conda install pandas
#from pandas import DataFrame as DF
from pandas import read_sql as read_db
from numpy import nan                            # conda install numpy
import quandl                                    # conda install quandl

# local imports
home = "/home/mcollier/ONYX/wealth/portfolio"
local_paths = [os.path.join(home, "scripts"), ]
sys.path.insert(0, [p for p in local_paths if p not in sys.path])
import common


##############################################################################
# Local User Function Definitions
##############################################################################

def make_weekly_data(con, ticker_id, begin_date, today):
    """
    INPUTS:
        con (mysql) - pymysql database connection
        ticker_id (int) - Ticker id number from symbols table
        begin_date (str) - Last price date in series. Iso8601 standard format
        today (str) - Today's date in iso8601 standard format
    OUTPUT:
        Returns a pandas dataframe resampled to weekly
    """

    sql = """SELECT * FROM daily_data WHERE symbol_id="{}"
             AND price_date BETWEEN "{}" AND "{}";"""
    df = read_db(sql.format(ticker_id, begin_date, today), con,
                 index_col="price_date", coerce_float=True,
                 parse_dates=["price_date", ])
    df = df.drop("id", axis=1)
    df.fillna(value=nan, inplace=True)
    o = df.open.resample('1W-FRI').ohlc().open
    h = df.high.resample('1W-FRI').max()
    l = df.low.resample('1W-FRI').min()
    c = df.close.resample('1W-FRI').ohlc().close
    v = df.volume.resample('1W-FRI').sum()
    ed = df.ex_dividend.resample('1W-FRI').sum()
    sr = df.split_ratio.resample('1W-FRI').cumprod()
    ao = df.adj_open.resample('1W-FRI').ohlc().open.rename("adj_open")
    ah = df.adj_high.resample('1W-FRI').max()
    al = df.adj_low.resample('1W-FRI').min()
    ac = df.adj_close.resample('1W-FRI').ohlc().close.rename("adj_close")
    av = df.adj_volume.resample('1W-FRI').sum()

    data = [o, h, l, c, v, ed, sr, ao, ah, al, ac, av]
    weekly = concat(data, axis=1)

    return weekly


def data_replace(con, now, vendor_id, ticker_id, data, time_span):
    """
    INPUTS:
        con (mysql) - pymysql database connection
        now (datetime) - Python datetime object in Central Time
        vendor_id (str) - For example,'1' for Yahoo Finance
        ticker_id (int) - Ticker id number from symbols table
        data (list) - List of tuples (ohlcv...)
        time_span (str) - Specify "daily" or "weekly"
    OUTPUT:
        Appends vendor ID and symbol ID to data. Adds result
        to either daily_data or weekly_data table.
    """

    # Amend the data to include the vendor ID and symbol ID
    data = [(ticker_id,) + d + (now, vendor_id) for d in data]

    # Create the insertion strings
    table = "{}_data".format(time_span)  # time_span = "daily"
    cols = ["symbol_id","price_date","open","high","low","close","volume",
            "ex_dividend","split_ratio","adj_open","adj_high","adj_low",
            "adj_close","adj_volume","last_update", "vendor_id"]
    header = ",".join(cols)
    inserts = ("%s,"*len(cols))[:-1]
    sql = "REPLACE INTO {} ({}) VALUES ({})".format(table, header, inserts)

    # REPLACE the data into a MySQL database table
    with con.cursor() as cur:
        cur.executemany(sql, data)
        results = con.commit()

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
    p.add_argument("-D", "--no_daily", action="store_true",
        help="suppress daily download if true")
    p.add_argument("-N", "--no_insert", action="store_true",
        help="suppress insertion of data into db tables if true")
    p.add_argument("-R", "--no_report", action="store_true",
        help="option to suppress reporting at end")
    p.add_argument("-W", "--no_weekly", action="store_true",
        help="suppress weekly resampling if true")
    args = p.parse_args()


    try:

        # set up functions, parameters, and other needed objects
        vprint = print if args.verbose else lambda *a, **k: None
        warnings.filterwarnings("ignore")  # ignore trunc from Decimal(19,4)
        wpack = ["WARNING(S):\n", 0]
        epack = ["ERROR(S):\n", 0]
        now = datetime.today()
        today = now.date().isoformat()
        dt = lambda i: i.date().isoformat()
        d4 = lambda x: Decimal(x).quantize(Decimal('0.0000'), rounding=RHE)
        tuplefy = lambda df: [ tuple([dt(i)] + [d4(c) for c in r])
                               for i,r in df.iterrows()]
        quandl.ApiConfig.api_key = conf["quandl_key"]
        con = common.get_connection(conf)

        # obtain a list of tickers and it's length
        lentickers, tickers = common.read_tickers(con, args.single)
    #    (lentickers, tickers) = common.read_tickers(con, "")
    #    https://www.quandl.com/search?query=
    #    broad = ["NIKKEI/SI300", "TSE/1458",   # Japanese Nikkei 225
    #             "INDEXHANGSENG:HSI",   # Chinese Hang Seng
    #             "INDEXDB:3BTI",        # German DAX
    #             "INDEXFTSE:UKX",       # UK FTSE
    #             "INDEXCBOE:TNX",       # 10-yr Bonds
    #             "INDEXCBOE:VIX",       # Volatility Index
    #             "INDEXSP:.INX"]        # US S&P 500,              "WIKI/SPW"

        # Loop over the tickers
        t_keys = list(tickers.keys())
        t_keys.sort()
        count = 0
        t00 = time()
        for t_key in t_keys:  # t_key = "WMT"
            vprint("Working on {}".format(t_key))
            t0 = time()

            # get the ticker's information
            if tickers[t_key][1] == 'i':  continue
            ticker_id, flag, name, sector, last_update = tickers[t_key]

            if not args.no_daily:  # pull daily data for Quandl with API
                t1 = time()
                sleep(1.3)  # throttling (Quandl allows rate of 300/10min)
                # average 2.36s each on 20171020 with sleep(1.5)
                # with sleep(1.3),
                # ave(s) @ 2.18, 2.06, 2.23, 2.14, 2.19, 2.21, 2.29
                instrument = "WIKI/" + t_key.replace('.', '_')
                begin_date = common.get_last_price_date(con, "daily_data",
                                                        ticker_id)
                begin_date = common.get_next_day(begin_date)
                daily = quandl.get(instrument, start_date=begin_date,
                                   end_date=today, collapse="daily")

                # if daily data not complete for ticker, warn and skip
                if daily.empty:
                    warn_msg = "Data load aborted for {}\n".format(t_key)
                    wpack = common.handle(warn_msg, wpack)
                    continue

                daily = tuplefy(daily)
                if not args.no_insert:
                    data_replace(con, now, '1', ticker_id, daily, "daily")

                vprint("daily data took {:.2f}s".format(time()-t1))

            if not args.no_weekly:  # calculate custom weekly data
                t2 = time()
                begin_date = common.get_last_price_date(con, "weekly_data",
                                                        ticker_id)
                weekly = make_weekly_data(con, ticker_id,
                                          common.get_dotw("prev", "Sunday",
                                                          begin_date),
                                          today)
                weekly = tuplefy(weekly)
                if not args.no_insert:
                    data_replace(con, now, '1', ticker_id, weekly, "weekly")
                vprint("weekly data took {:.2f}s".format(time()-t2))

            # "update" only if both were updated... must rethink!
            if not args.no_daily or not args.no_weekly:
                common.update_tickers(con, "a", [t_key,], now)

            count += 1
            vprint("({}/{}) Finished {} in {:.2f}s".format(count, lentickers,
                                                           t_key, time() - t0))

    except Exception as err_msg:

        epack = common.handle(err_msg, epack)
        vprint("*({:3d}/{:3d}): ERROR for {} ({}) ".format(count,
                                                           lentickers,
                                                           t_key, name))
        vprint(epack[0])


    ##########################################################################
    # reporting
    ##########################################################################

    if not args.no_report:

        tf = time()
        msg  = "Total Time is {:.2f}m\n".format((tf-t00)/60)
        msg += "INFO: Ticker(s): {}\n".format("All" if len(tickers) > 1
                                                    else t_keys[0])
        msg += "INFO: DB Insertion: {}\n".format(not args.no_insert)
        msg += "INFO: Through Date: {}\n".format(today)

        if not epack[1]: msg += "ERRORS: None\n"
        else: msg += "ERRORS:\n{}\n".format(epack)

        logfile = home + "/logs/{}_data.log".format(today)
        f = open(logfile, "a")
        f.write("#"*79 + '\n')
        f.write(now.ctime() + '\n')
        f.write(msg)
        f.close()

    else:

        msg = "...reporting turned off!\n"


    # finally
    con.close()
    vprint(msg)

