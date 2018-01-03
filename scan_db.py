#!/home/mcollier/miniconda3/bin/python
# -*- coding: utf-8 -*-
__author__ = "Matthew Collier"
__version__ = "0.5"

# Typical use cases:
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/scan.py -v
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/scan_db.py -s WMT
#hf> /media/mcollier/ONYX/ONYX/W/portfolio/scripts/scan_db.py -d 2017-11-03


##############################################################################
# TBD:
#
# * Add total volume and range for S&P500 broad metrics
# * relativize the entire project's paths
# * improve documentation
#
##############################################################################

# standard python library imports
import argparse
import collections
import datetime as dt
from math import pi
import os

# third party python imports
from bs4 import BeautifulSoup                   # conda install beautifulsoup4
from bokeh.embed import file_html               # conda install bokeh
from bokeh.io import output_file, save
from bokeh.layouts import column
from bokeh.models import Legend
from bokeh.plotting import figure, show
from bokeh.resources import CDN, INLINE
from pymysql import connect                     # conda install pymysql
from pymysql.cursors import DictCursor
from pandas import read_sql_query as rsql       # conda install pandas

# overriding with local imports
home = "/home/mcollier/ONYX/W/portfolio/scripts"
os.chdir(home)
import common


##############################################################################
# Local User Function Definitions
##############################################################################

def get_broads(d_con, span, price_date, tids=[]):
    """
    INPUTS:
        d_con (mysql) - A pymysql database connection with dict() return
        span (str) - 'daily' or 'weekly'
        price_date (str) - Data in ISO 8601 standard as YYYY-MM-DD
        tids [list] - id's of tickers to include with default of all
    OUTPUT:
        Dictionary with metrics as keys, and sums as values
    """
    #span = "daily",
    #price_date = "2017-10-31",
    #tids = [1, 2, 5, 10, 20, 50, 100]

    tids = [str(tid) for tid in tids]
    cols = ["gt12", "gt26", "gt50", "ad"]  # which metrics to sum()
    sql = ["SELECT " + ("SUM({}) AS ##,"*len(cols)).format(*cols)[:-1],
           "FROM {}_metrics".format(span),
           "WHERE price_date='{}'".format(price_date),
           "" if not tids else "AND symbol_id in {}".format(tuple(tids)),
           ';']
    sql = " ".join(sql)
    sql = sql.replace("##", "{}").format(*cols)
    with d_con.cursor() as cur:
        cur.execute(sql)
        broads = cur.fetchone()

    return broads


def get_sectors(n_con):
    """
    INPUTS:
        n_con (mysql) - A "normal" pymysql database connection
    OUTPUT:
        Dictionary with sectors as keys, and lists of ticker_id's as values
    """
    sectors = collections.defaultdict(list)
    with n_con.cursor() as n_cur:
        n_cur.execute("SELECT sector,id FROM symbols where flag='a';")
        results = n_cur.fetchall()
    for result in results:
        sectors[result[0]].append(result[1])

    return sectors


def get_longs(n_con, span, price_date):
    """
    """
    sql = ["SELECT symbol_id FROM {}_metrics".format(span),
           "WHERE price_date='{}'".format(price_date),
           "AND impulse>0",                                # positive impulse
           "AND gt26<0"]                                   # below value zone
    with n_con.cursor() as n_cur:
        n_cur.execute(" ".join(sql))
        results = n_cur.fetchall()

    return [result[0] for result in results]


def get_shorts(n_con, span, price_date):
    """
    """
    sql = ["SELECT symbol_id FROM {}_metrics".format(span),
           "WHERE price_date='{}'".format(price_date),
           "AND impulse<0",                                # negative impulse
           "AND gt26>0"]                                   # above value zone
    with n_con.cursor() as n_cur:
        n_cur.execute(" ".join(sql))
        results = n_cur.fetchall()

    return [result[0] for result in results]


def get_ticker_info(d_con, tid):
    """
    INPUTS:
        d_con (mysql) - A pymysql database connection with dict() return
        tid (int) - id of ticker
    OUTPUT:
        Dictionary with ...
    """

    sql = ["SELECT id,ticker,name,sector FROM symbols",
           "WHERE id={}".format(tid),
           ";"]
    with d_con.cursor() as d_cur:
        d_cur.execute(" ".join(sql))
        info = d_cur.fetchone()

    return info


def report_individuals(n_con, price_date, tickers=[]):  # tickers=singles
    """
    """
    msg = "{:>3s}, {:>8s}, {:>7s}, {:>6s}, {:5d}, {:5.3f}, {:8.3f}, {:<s}\n"
    url = "<a href='./{}/{}{}.html'> {:s} </a>"
    n_cur = n_con.cursor()
    html = ""
    for t in tickers:
        #t={'id': 7,'name':'Abbott Laboratories','sector':'Health Care','ticker': 'ABT'}
        n_cur.execute( ("SELECT count(*) FROM daily_data "
                        "WHERE symbol_id={}").format(t['id']) )
        count = n_cur.fetchone()[0]
        n_cur.execute( ("SELECT atr13 FROM daily_metrics WHERE symbol_id={} "
                        "AND price_date='{}';").format(t['id'], price_date) )
        atr13 = n_cur.fetchone()[0]
        n_cur.execute( ("SELECT close FROM daily_data WHERE symbol_id={} "
                        "AND price_date='{}'").format(t['id'], price_date) )
        close = n_cur.fetchone()[0]
        ticker_id = str(t['id']).zfill(3)
        weekly_url = url.format(price_date, 'w', t["ticker"], "weekly")
        daily_url = url.format(price_date, 'd', t["ticker"], "daily")
        html += msg.format(ticker_id, weekly_url, daily_url, t["ticker"],
                           count, atr13/close, close, t["sector"])
    return html


def bokeh_pages(d_con, span, price_date, att, tickers=[]):
    """
    INPUTS:
        d_con (pymysql) - MySQL connection returning a dictionary
        span (str) -
        price_date (str) -
        att (str) -
        tickers (list) - List containing ticker IDs
    OUTPUT:
        HTML files
    """
    # set up variables
    w = 12*60*60*1000  # half day in ms
    if span == "weekly": w *= 7
    TOOLS = "crosshair,hover,pan,wheel_zoom,box_zoom,reset,save"
    data_sql = """SELECT d.price_date AS price_date, d.open AS open,
                d.high AS high, d.low AS low,
                d.close AS close, d.volume AS volume
             FROM {}_data d
             INNER JOIN symbols sym ON d.symbol_id = sym.id
             WHERE sym.ticker='{}' AND d.price_date>"{}"
             ORDER BY d.price_date ASC;"""
    metrics_sql = """SELECT price_date, ema12, ema26, atr13,
                            macdf, macds, macdh, force2
                     FROM {}_metrics m
                     INNER JOIN symbols sym ON m.symbol_id = sym.id
                     WHERE sym.ticker='{}' AND m.price_date>"{}"
                     ORDER BY m.price_date ASC;"""

    if span == "weekly":
        price_date = common.get_dotw("next", "Friday", from_date=price_date)
        offset = 52*7 + 1
    else:
        offset = 60
    time_obj = dt.datetime.strptime(price_date, "%Y-%m-%d")
    start_date = (time_obj - dt.timedelta(days=offset)).date().isoformat()

    for ticker in tickers:  # tickers = longs  # tickers = shorts

        # set up in-loop variables
        info = get_ticker_info(d_con, ticker)  # ticker = 69
        t = info["ticker"]
        D = rsql(data_sql.format(span, t, start_date),
                       con=n_con, index_col="price_date")
        D["date"] = D.index.to_series()
        inc = D.close > D.open
        dec = D.open > D.close
        M = rsql(metrics_sql.format(span, t, start_date),
                       con=n_con, index_col="price_date")
        M["date"] = M.index.to_series()

        # candlestick plot
        title = "{}, {} {}".format(price_date, span, t)
        p1 = figure(x_axis_type="datetime", tools=TOOLS, plot_height=618,
                    plot_width=1000, title = title+" Candlestick")
        p1.xaxis.major_label_orientation = pi/4
        p1.grid.grid_line_alpha = 0.3
        p1.segment(D.date, D.high, D.date,
                   D.low, line_width=2, color="black")
        p1.vbar(D.date[inc], w, D.open[inc], D.close[inc],
                fill_color="#D5E1DD", line_color="black")
        p1.vbar(D.date[dec], w, D.open[dec], D.close[dec],
                fill_color="#F2583E", line_color="black")

        # bokeh.pydata.org/en/latest/docs/user_guide/tools.html#hovertool

        # ema26, ema12, 1atr, 2atr, 3atr linear overlays
        p1.line(M.date, M.ema26 + 3*M.atr13, legend="3 ATR", color="gray")
        p1.line(M.date, M.ema26 + 2*M.atr13, legend="2 ATR", color="dimgray")
        p1.line(M.date, M.ema26 + M.atr13, legend="1 ATR", color="black")
        p1.line(M.date, M.ema26, legend="ema26", color="green", line_width=4)
        p1.line(M.date, M.ema12, legend="ema12", color="green", line_width=2)
        p1.line(M.date, M.ema26 - M.atr13, legend="-1 ATR", color="black")
        p1.line(M.date, M.ema26 - 2*M.atr13, legend="-2 ATR", color="dimgray")
        p1.line(M.date, M.ema26 - 3*M.atr13, legend="-3 ATR",color="gray")
        p1.legend.location = "top_left"
        p1.legend.border_line_width = 2
        p1.legend.background_fill_color = "aliceblue"

        # second plot for macX family
        p2 = figure(x_axis_type="datetime", tools=TOOLS, plot_height=250,
                    plot_width=1000, title = title+" MACDx")
        p2.vbar(x=M.date, width=w, top=M.macdh, color="#CAB2D6")
        p2.line(M.date, M.macds, legend="macds", color="black", line_width=2)
        p2.line(M.date, M.macdf, legend="macdf", color="black", line_width=1)
        p2.legend.location = "top_left"
        p2.legend.border_line_width = 2
        p2.legend.background_fill_color = "aliceblue"

        # work with html
        report = file_html(column(p1,p2), resources=CDN, title=title)
        t_slash = t.replace('.', '/')
        url1 = "https://www.bloomberg.com/quote/{}:US".format(t_slash)
        url2 = "https://finance.google.com/finance?q={}".format(t)
        url3 = "https://www.reuters.com/finance/stocks/overview/{}".format(t)
        url4 = "http://shortsqueeze.com/?symbol={}".format(t.replace('.', ''))
        new = ("<h2> Research Links </h2>"
               "<ul><li><a href='{}'> Bloomberg </a>"
                   "<li><a href='{}'> Google </a>"
                   "<li><a href='{}'> Reuters </a>"
                   "<li><a href='{}'> ShortSqueeze </a>"
               "</ul>").format(url1, url2, url3, url4)
        miso = BeautifulSoup(report, "html.parser")
        miso.body.insert(0, BeautifulSoup(new, "html.parser"))
        with open("{}{}.html".format(span[0], ticker), "w") as f:
            f.write(str(miso))


##############################################################################
# Treat this file as a script if invoked as __main__
##############################################################################

if __name__ == "__main__":

    # load configuration from commented JSON into dictionary
    conf = common.get_config("/etc/local/hf.conf")

    # assign parsed commandline values to working objects
    p = argparse.ArgumentParser()
    p.add_argument("-d", "--date", default="today",
        help="work with a particular date")
    p.add_argument("-s", "--select", default="",
        help="pull select ticker(s)... i.e. -s 69,488")
    p.add_argument("-v", "--verbose", action="store_true",
        help="print extra information on stdout")
#    p.add_argument("-N", "--no_insert", action="store_true",
#        help="suppress db insertion if true")
#    p.add_argument("-R", "--no_report", action="store_true",
#        help="option to suppress reporting at end")
    args = p.parse_args()
    vprint = print if args.verbose else lambda *a, **k: None

    d_con = connect(host=conf["db_host"],     db=conf["db_name"],
                    user=conf["db_user"], passwd=conf["db_pass"],
                    cursorclass=DictCursor)
    n_con =  connect(host=conf["db_host"],     db=conf["db_name"],
                    user=conf["db_user"], passwd=conf["db_pass"])

    if args.date == "today": price_date=dt.datetime.today().date().isoformat()
    else: price_date = args.date  # price_date = "2017-11-10"


    ##########################################################################
    # S&P500 and Sector indicators, World Research
    ##########################################################################

    html = """<html><head></head><body>
    <center><em><b><h1><hr><hr>
    "Every true idealist is after money ---because money means freedom,
    and freedom, in the final analysis, means life."  - Oscar Wilde
    <hr><hr></h1></b></em></center>
    <h2> Verify Processing </h2>
    <a href="../logs> Log Directory </a>
    <h2> Global Sweep </h2>
    <ul>
    <li> CNN <a href="http://money.cnn.com/data/world_markets/europe/"> World Markets </a> Map & News
    <li> <a href="https://www.marketwatch.com/topics/columns/need-to-know"> Headlines </a> (a brief glance only)
    </ul>\n"""
    head = "{:>6s}, {:>4s}, {:>4s}, {:>4s}, {:>4s}"
    head = head.format("span", "gt50", "gt26", "gt12", "ad")

    # report S&P500-wide indicators
    html += ("<h2> S&P500 </h2><pre>")
    html += head + "<br>"
    row = "{:>6s}: {:4d}, {:4d}, {:4d}, {:4d}<br>"
    for span in ["daily", "weekly"]:
        pop = get_broads(d_con, span, price_date)
        html += row.format(span, int(pop['gt50']), int(pop['gt26']),
                           int(pop['gt12']), int(pop['ad']))

    # report sector indicators
    html += ("</pre><h2> GISC Sectors </h2>\n<pre>")
    html += (head + ", {:>3s}, sector<br>".format("n"))
    row = "{:>6s}: {:4d}, {:4d}, {:4d}, {:4d}, {:3d}, {}<br>"
    for span in ["daily", "weekly"]:
        sectors = get_sectors(n_con)
        for sector, values in sectors.items():
            m = get_broads(d_con, span, price_date, values)
            html += row.format(span, int(m['gt50']), int(m['gt26']),
                               int(m['gt12']), int(m['ad']),
                               len(values), sector)
        html += "<br>"


    ##########################################################################
    # Individual short/long candidates
    ##########################################################################

    head = "{:>3s}, {:>8s}, {:>7s}, {:>6s}, {:>5s}, {:>5s}, {:>8s}, {:<s}\n"
    head = head.format("ID", "WEEKLIES", "DAILIES", "TICKER",
                       "COUNT", "CoV", "CLOSE($)", "SECTOR")

    if not args.select:

        # work with shorts
        shorts = get_shorts(n_con, "daily", price_date)
        esses = [get_ticker_info(d_con, short) for short in shorts]
        html += ("<h2> Daily Shorts </h2><pre>")
        html += head
        html += report_individuals(n_con, price_date, esses)

        # work with longs
        longs = get_longs(n_con, "daily", price_date)
        elles = [get_ticker_info(d_con, long) for long in longs]
        html += ("</pre><h2> Daily Longs </h2><pre>")
        html += head
        html += report_individuals(n_con, price_date, elles)

    else:

        # work with singles
        select = args.select.split(',')
        select = [int(s) for s in select]
        singles = [get_ticker_info(d_con, s) for s in select]
        html += ("</pre><h2> Daily Single(s) </h2><pre>")
        html += head
        html += report_individuals(n_con, price_date, singles)

    with open("inspection/{}.html".format(price_date), "w") as f:
        f.write(html + "<br><br></pre></body></html>")


    ##########################################################################
    # Generate Inpspection/Research Webpages
    ##########################################################################

    # set up data
    date_dir = os.path.join(home, "inspection/{}".format(price_date))
    if not os.path.exists(date_dir): os.makedirs(date_dir)
    os.chdir(date_dir)

    # create inspection/research web pages
    for span in ["weekly", "daily"]:
        if not args.select: attitudes = {"longs": longs, "shorts": shorts}
        else: attitudes = {"select": select}
        for att in attitudes.keys():
            bokeh_pages(d_con, span, price_date, att, tickers=attitudes[att])


    # finally...
    d_con.close()
    n_con.close()




