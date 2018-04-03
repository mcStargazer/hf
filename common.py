#!/home/mcollier/miniconda3/bin/python
# -*- coding: utf-8 -*-

##############################################################################
# TBD:
#
#   improve logging
#   improve documentation
#   refactor get_last_sunday() and get_next_saturday() to get_dotw()
#
##############################################################################

# standard python library imports
import datetime as dt
import json
import sys
from time import sleep, time
import traceback

# third party library imports
from pandas import DataFrame as DF               # conda install pandas
from pymysql import connect                      # conda install pymysql


##############################################################################
# Local Test Data Definitions
##############################################################################

# dict of time-series of Yahoo Walmart prices for pandas ingestion
wmt = {"index": ["2017-09-18", "2017-09-19", "2017-09-20",
                 "2017-09-21", "2017-09-22", "2017-09-25"],
        "open": [  80.21,   80.18,   80.17,   80.57,   79.89,   78.92],
        "high": [  81.12,   80.46,   80.56,   80.57,   80.08,   80.10],
         "low": [  79.95,   79.70,   79.87,   79.72,   79.25,   78.86],
       "close": [  80.00,   80.05,   80.50,   80.01,   79.53,   79.15],
   "adj_close": [  80.00,   80.05,   80.50,   80.01,   79.53,   79.15],
      "volume": [8176100, 6174400, 5318200, 6430300, 5755300, 8506800]}


# day-per-row format of Yahoo Walmart prices for spreadsheet ingestion
wmt_str = """
date open high low close adj_close volume
2017-09-18 80.21 81.12 79.95 80.00 80.00 8176100
2017-09-19 80.18 80.46 79.70 80.05 80.05 6174400
2017-09-20 80.17 80.56 79.87 80.50 80.50 5318200
2017-09-21 80.57 80.57 79.72 80.01 80.01 6430300
2017-09-22 79.89 80.08 79.25 79.53 79.53 5755300
2017-09-25 78.92 80.10 78.86 79.15 79.15 8506800
"""


##############################################################################
# Local User Function Definitions
##############################################################################

def get_config(s):

    """
    INPUTS:
        JSON configuration filename as string. Comments consisting
        of whole lines may be inserted if first character is '#'.
    OUTPUT:
        Look-up table as a dictionary
    """

    with open(s, 'r') as configuration_file:
        conf = configuration_file.readlines()

    return json.loads("".join([r for r in conf if r[0] != '#']))


def get_connection(conf):

    """
    INPUTS:
        JSON configuration file as string. Comments consisting
        of whole lines may be inserted if first character is '#'.
    OUTPUT:
        Mysql connection
    """

    # Connect to the MySQL instance
    con = connect(  host=conf["db_host"],
                    user=conf["db_user"],
                  passwd=conf["db_pass"],
                      db=conf["db_name"])
    return con


def read_tickers(con, ticker='', comparison='<>"i"'):
    """
    INPUTS:
        con (mysql) - pymysql database connection
        ticker (str) - Ticker symbol on the S&P 500
        comparison (str) - Default is to ignore inactivated tickers, 'i'.
                           Other flags are 'a' for active, and 'n' for new.
    OUTPUT:
        Returns a tuple with the count, and ticker data in dictionary form
        with ticker symbols as keys.
    """

    data = dict()
    sql = """SELECT ticker, id, flag, name, sector, last_update
             FROM symbols WHERE flag{}""".format(comparison)
    if ticker: sql += " AND ticker='{}'".format(ticker)
    with con.cursor() as cur:
        number = cur.execute(sql)
        rows = cur.fetchall()

    for row in rows:
        data[row[0]] = row[1:]

    return (number, data)


def update_tickers(con, flag, tickers, dt):
    """
    INPUTS:
        con (mysql) - pymysql database connection
        flag (str) - Ticker status ('a','i','n') -> (active, inactive, new)
        tickers (list) - List of ticker symbols on the S&P 500 to update.
        dt (str) - Date of last (this) update is iso8601 format.
    OUTPUT:
        Side effect of updating the symbols table.
    """

    sql = '''UPDATE symbols SET flag="{}", last_update="{}"
             WHERE ticker="{}"'''

    with con.cursor() as cur:
        for ticker in tickers:
            cur.execute(sql.format(flag, dt, ticker))
        con.commit()


def handle(msg, epack):
    """
    INPUTS:
        msg (str) - A new error string to pack into the accumulator
        epack (list) - Predefined accumulator for error strings and counts
    OUTPUT:
        epack (list) - Same as input, with format [str, int]
    """

    tb = sys.exc_info()[2]
    if tb:
        tbinfo = traceback.format_tb(tb)[0]
        trace = "TRACEBACK:\n" + tbinfo
    else:
        trace = ''

    epack[0] += "MESSAGE:\n" + str(msg) + "\n" + trace
    epack[1] += 1

    return epack


def get_next_day(date, encode="iso8601"):
    """
    INPUT:
        date (str) - A day encoded as an iso8601 formatted string: YYYY-MM-DD
        encode (str) - Options are either 'iso8601' or 'python date'.
    OUTPUT:
        Return the next day's date (default iso8601 format)
    """
    date = dt.datetime.strptime(date, "%Y-%m-%d")
    date = (date + dt.timedelta( days=1 ))
    if encode == "iso8601":
        return date.isoformat()[:10]
    elif encode == "python date":
        return date
    else:
        return "unknown date encoding"


def get_last_price_date(con, table, ticker_id):
    """
    INPUTS:
        con (mysql) - pymysql database connection
        table (str) - Database table to query
        ticker_id (int) - Ticker id number from symbols table
    OUTPUT:
        Last price date in ticker's time series. Iso8601 standard format
    """

    sql = "SELECT max(price_date) FROM {} WHERE symbol_id={}"
    with con.cursor() as cur:
        cur.execute(sql.format(table, ticker_id))
        last_date = cur.fetchone()[0]
        if not last_date: last_date = "2001-01-01"
        else: last_date = last_date.isoformat()

    return last_date


def get_dotw(sign, dotw, from_date="2000-01-01", encode="iso8601"):
    """
    INPUT:
        sign (str) - Find next, or previous day-of-the-week ("next", "prev")
        dotw (str) - Day-of-the-week to find ("Sunday",..."Saturday")
        from_date (str) - Starting date given as "YYYY-MM-DD"
        encode (str) - Return type ("iso8601", "python date")
    OUTPUT:
        Nearby day-of-the-week from given date (default iso8601 format). If
        you ask it to return the previous or next dotw and the from_date is
        the same dotw, then it returns itself. I.e. Give it a from_date that
        is a Monday, and ask it for the previous Monday, and you'll just get
        back the date you gave it.
    """
    # sign, dotw, from_date = "prev", "Monday", "2017-11-15"
    # sign, dotw, from_date = "next", "Friday", "2017-11-15"
    DOTW = {"Monday": 0,  "Tuesday": 1, "Wednesday": 2, "Thursday": 3,
            "Friday": 4, "Saturday": 5,    "Sunday": 6}
    if ((dotw not in DOTW.keys()) or
        (sign not in ["prev", "next"]) or
        (encode not in ["iso8601", "python date"])):
        return "Unknown parameter(s) requested!\n"
    offset = DOTW[dotw]
    date = dt.datetime.strptime(from_date, "%Y-%m-%d")
    if sign == "prev":
        date = (date - dt.timedelta( days=( (date.weekday() - offset) % 7) ))
    else:
        date = (date + dt.timedelta( days=( (offset - date.weekday()) % 7) ))
    if encode == "iso8601": return date.isoformat()[:10]
    else: return date


##############################################################################
# Treat this file as a script if invoked as __main__
##############################################################################

if __name__ == "__main__":

    # TEST: load data
    daily = DF.from_csv("wmt_daily_df.csv")
    df = DF(data=wmt, index=wmt["index"])
    df = df.drop("index", axis=1)

    # TEST: verification of get_dotw()
    date = dt.date.today().isoformat()
    days = ["Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday"]
    signs = ["prev", "next"]
    for sign in signs:
        for day in days:
            print("{} {}: {}".format(sign, day, get_dotw(sign, day, date)))
        
