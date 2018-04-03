#!/home/mcollier/miniconda3/bin/python
# -*- coding: utf-8 -*-

# Typical use cases:
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/get_tickers.py -v
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/get_tickers.py -vN
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/get_tickers.py -h


##############################################################################
# TBD:
#
#   improve logging
#   improve documentation
#   table of permanent members: ^N225, ^HSI, ^GDAXI, ^FTSE, ^TNX, ^VIX, ^GSPC
#   add code snippet to reactivate a ticker
#   add table of ticker deltas: id, symbol_id, flag, ticker, name, date
#   harmonize outputs for inactivated and inserted
#
##############################################################################


# standard python library imports
import argparse
import datetime
import os
import sys

# third party python imports
from bs4 import BeautifulSoup as miso        # conda install beautifulsoup4
from requests import get                     # conda install requests

# local imports
home = "/home/mcollier/ONYX/wealth/portfolio"
local_paths = [os.path.join(home, "scripts"), ]
sys.path.insert(0, [p for p in local_paths if p not in sys.path])
import common


##############################################################################
# Local User Function Definitions
##############################################################################

def get_wiki_tickers(url, now, then, epack):
    """
    INPUTS:
       url (str) - URL for bulk S&P500 ticker download from Wikipedia
       now (date) - Current datetime.date object
       then (date) - Beginning datetime.date object (a Sunday by design)
       epack (list) - Predefined accumulator for error strings and counts
    OUTPUT:
       Returns dictionary of tickers and attributes, and an epack with
       any error strings and counts.
    """

    try:

        # prepare the soup
        response = get(url)
        soup = miso(response.text, "lxml")  # conda install lxml

        # select first table, ignore header ([1:]), use CSS Selector syntax
        symbols_list = soup.select("table")[0].select("tr")[1:]

        symbols = dict()
        for i, symbol in enumerate(symbols_list):
            tds = symbol.select("td")
            ticker = tds[0].select('a')[0].text
            name = tds[1].select('a')[0].text
            sector = tds[3].text
            symbols[ticker] = (ticker, "stock", name,
                               sector, "USD", now, then)

    except Exception as err_msg:

        epack = common.handle(err_msg, epack)

    return (symbols, epack)


def insert_tickers(con, data, epack):
    """
    INPUTS:
       con (mysql) - pymysql database connection
       data (tuples) - Tuple of tuples
       epack (list) - Predefined accumulator for error strings and counts
    OUTPUT:
       Side effect of insertion of symbols and their parameters into database,
       and returns an epack with any error strings and counts.
    """

    try:

        # Create the SQL statement
        columns = """ticker, instrument, name, sector, currency,
                     inserted, last_update"""
        inserts = ("%s, " * 7)[:-2]
        sql = "INSERT INTO symbols ({}) VALUES ({})".format(columns, inserts)

        # populate database table, and exit cleanly
        with con.cursor() as cur:
            cur.executemany(sql, data)
            con.commit()

    except Exception as err_msg:

        epack = common.handle(err_msg, epack)

    return epack


##############################################################################
# Treat this file as a script if invoked as __main__
##############################################################################

if __name__ == "__main__":

    # load configuration from commented JSON into dictionary
    conf = common.get_config("/etc/local/hf.conf")

    # parse the commandline for any goodies
    p = argparse.ArgumentParser()
    p.add_argument("-v", "--verbose", action="store_true",
        help="print extra information on stdout")
    p.add_argument("-N", "--no_insert", action="store_true",
        help="suppress insertion of data into db tables if True")
    p.add_argument("-R", "--no_report", action="store_true",
        help="suppress reporting at end if True")
    args = p.parse_args()


    try:

        # set up functions, parameters, and other needed objects
        vprint = print if args.verbose else lambda *a, **k: None
        epack = ["ERROR(S):\n", 0]
        wpack = ["WARNING(S):\n", 0]
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        present = datetime.datetime.now()
        today = present.date().isoformat()
        BoT = "2000-01-03"  # Beginning of Time
        con = common.get_connection(conf)

        # get current from web, read previous from db
        (current_active, epack) = get_wiki_tickers(url, present.date(),
                                                   BoT, epack)
        c_keys = list(current_active.keys())
        c_keys.sort()
        (p_count, previous_active) = common.read_tickers(con)
        p_keys = list(previous_active.keys())
        p_keys.sort()
        (i_count, inactive) = common.read_tickers(con, comparison='="i"')
        i_keys = list(inactive.keys())
        i_keys.sort()

        # make lists of new inactive and new active instruments
        new_inactive_keys = [p_key for p_key in p_keys if p_key not in c_keys]
        new_keys = [c_key for c_key in c_keys if c_key not in p_keys]
        new_active = {}
        for new_key in new_keys:
            if new_key not in i_keys:
                new_active[new_key] = current_active[new_key]
            else:
                warn_msg = "Reactivate {}? (verify last_update!)\n".format(new_key)
                wpack = common.handle(warn_msg, wpack)
                vprint(warn_msg)

        # update the MySQL database if not no_insert
        if not args.no_insert:
            common.update_tickers(con, "i", new_inactive_keys, present.date())
            epack = insert_tickers(con, tuple(new_active.values()), epack )
            common.update_tickers(con, "n", new_keys, BoT)


    except Exception as err_msg:

        epack = common.handle(err_msg, epack)


    ##########################################################################
    # reporting
    ##########################################################################

    if not args.no_report:

        msg = ''

        if args.no_insert:
            msg += "INFO: DB Insertion Deactivated\n"

        if new_inactive_keys or new_keys:
            changes = "#Recent_changes_to_the_list_of_S.26P_500_Components"
            msg += ("INFO: Verify any activations/inactivations"
                    " below at\nINFO: {}\n").format(url+changes)
            for i_key in new_inactive_keys:
                printable = (previous_active[i_key][0],
                             previous_active[i_key][3],
                             previous_active[i_key][2])
                msg += "INACTIVATED: {}\n".format(printable)
            for n_key in new_keys:
                printable = (current_active[n_key][0],
                             current_active[n_key][3],
                             current_active[n_key][2])
                msg += "INSERTED: {}\n".format(printable)

        msg += ("SUMMARY: {} inactivated,\n"
                "         {} new\n").format(len(new_inactive_keys),
                                            len(new_keys))
        if not wpack[1]: msg += "WARNINGS: None\n"
        else: msg += "WARNINGS:\n{}\n".format(wpack[0])
        if not epack[1]: msg += "ERRORS: None\n"
        else: msg += "ERRORS:\n{}\n".format(epack[0])

        logfile = home + "/logs/{}_symbols.log".format(today)
        f = open(logfile, "a")
        f.write("#"*79 + '\n')
        f.write(present.ctime() + '\n')
        f.write(msg)
        f.close()

    else:

        msg = "...reporting turned off!\n"


    # finally
    con.close()
    vprint(msg)

