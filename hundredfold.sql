##############################################################################
# MAINTENANCE
##############################################################################

# scan for missing data
SET @pd = "2017-10-31";
SELECT price_date,count(*) AS count FROM daily_data
   WHERE price_date > @pd GROUP BY price_date;

# find missing tickers for a day
SET @pd = "2017-11-22";
SELECT id,ticker,flag,inserted,last_update FROM symbols
   WHERE id NOT IN (SELECT symbol_id FROM daily_data WHERE price_date=@pd);


# back off ALL data to a previous date (exclusively)
SET @pd = "2017-10-31";
DELETE FROM daily_data WHERE price_date > @pd;
DELETE FROM weekly_data WHERE price_date > @pd;
DELETE FROM daily_metrics WHERE price_date > @pd;
DELETE FROM weekly_metrics WHERE price_date > @pd;
UPDATE symbols SET last_update = @pd WHERE flag='a';

SELECT dd.symbol_id, dd.price_date FROM daily_data dd
INNER JOIN (SELECT symbol_id, MAX(price_date) AS last
            FROM daily_data GROUP BY symbol_id) AS grouped
ON dd.symbol_id=grouped.symbol_id AND dd.price_date = grouped.last;


##############################################################################
# INSTALLING MYSQL, and CREATING hundredfold and tables
##############################################################################

# linux> sudo apt-get install mysql-server
# linux> mysql -u root -p
CREATE DATABASE hundredfold;
USE hundredfold;
CREATE USER 'mcollier'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON hundredfold.* TO 'mcollier'@'localhost';
FLUSH PRIVILEGES;


CREATE TABLE `symbols` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `exchange_id` int(11) DEFAULT NULL,
  `flag` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `ticker` varchar(32) CHARACTER SET utf8 NOT NULL,
  `instrument` varchar(64) CHARACTER SET utf8 NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `sector` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `currency` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `inserted` datetime NOT NULL,
  `last_update` datetime NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `daily_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol_id` int(11) NOT NULL,
  `price_date` date NOT NULL,
  `open` decimal(19,4) DEFAULT NULL,
  `high` decimal(19,4) DEFAULT NULL,
  `low` decimal(19,4) DEFAULT NULL,
  `close` decimal(19,4) DEFAULT NULL,
  `volume` decimal(19,4) DEFAULT NULL,
  `ex_dividend` decimal(19,4) DEFAULT NULL,
  `split_ratio` decimal(19,4) DEFAULT NULL,
  `adj_open` decimal(19,4) DEFAULT NULL,
  `adj_high` decimal(19,4) DEFAULT NULL,
  `adj_low` decimal(19,4) DEFAULT NULL,
  `adj_close` decimal(19,4) DEFAULT NULL,
  `adj_volume` decimal(19,4) DEFAULT NULL,
  `last_update` datetime DEFAULT NULL,
  `vendor_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `symbol_date` (`symbol_id`,`price_date`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `weekly_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol_id` int(11) NOT NULL,
  `price_date` date NOT NULL,
  `open` decimal(19,4) DEFAULT NULL,
  `high` decimal(19,4) DEFAULT NULL,
  `low` decimal(19,4) DEFAULT NULL,
  `close` decimal(19,4) DEFAULT NULL,
  `volume` decimal(19,4) DEFAULT NULL,
  `ex_dividend` decimal(19,4) DEFAULT NULL,
  `split_ratio` float(19,4) DEFAULT NULL,
  `adj_open` decimal(19,4) DEFAULT NULL,
  `adj_high` decimal(19,4) DEFAULT NULL,
  `adj_low` decimal(19,4) DEFAULT NULL,
  `adj_close` decimal(19,4) DEFAULT NULL,
  `adj_volume` decimal(19,4) DEFAULT NULL,
  `last_update` datetime DEFAULT NULL,
  `vendor_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `symbol_date` (`symbol_id`,`price_date`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `daily_metrics` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol_id`  int(11)           NOT NULL,
  `price_date` date              NOT NULL,
  `ema12`      decimal(19,4) DEFAULT NULL,
  `ema26`      decimal(19,4) DEFAULT NULL,
  `ema50`      decimal(19,4) DEFAULT NULL,
  `force`      bigint(20)    DEFAULT NULL,
  `force2`     bigint(20)    DEFAULT NULL,
  `tr`         decimal(19,4) DEFAULT NULL,
  `atr13`      decimal(19,4) DEFAULT NULL,
  `macdf`      decimal(19,4) DEFAULT NULL,
  `macds`      decimal(19,4) DEFAULT NULL,
  `macdh`      decimal(19,4) DEFAULT NULL,
  `impulse`    tinyint(1)    DEFAULT NULL,
  `gt12`       tinyint(1)    DEFAULT NULL,
  `gt26`       tinyint(1)    DEFAULT NULL,
  `gt50`       tinyint(1)    DEFAULT NULL,
  `ad`         tinyint(1)    DEFAULT NULL,
  `updated`    date          DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `symbol_date` (`symbol_id`,`price_date`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `weekly_metrics` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol_id`  int(11) NOT NULL,
  `price_date` date NOT NULL,
  `ema12`      decimal(19,4) DEFAULT NULL,
  `ema26`      decimal(19,4) DEFAULT NULL,
  `ema50`      decimal(19,4) DEFAULT NULL,
  `force`      bigint(20)    DEFAULT NULL,
  `force2`     bigint(20)    DEFAULT NULL,
  `tr`         decimal(19,4) DEFAULT NULL,
  `atr13`      decimal(19,4) DEFAULT NULL,
  `macdf`      decimal(19,4) DEFAULT NULL,
  `macds`      decimal(19,4) DEFAULT NULL,
  `macdh`      decimal(19,4) DEFAULT NULL,
  `impulse`    tinyint(1)    DEFAULT NULL,
  `gt12`       tinyint(1)    DEFAULT NULL,
  `gt26`       tinyint(1)    DEFAULT NULL,
  `gt50`       tinyint(1)    DEFAULT NULL,
  `ad`         tinyint(1)    DEFAULT NULL,
  `updated`   date          DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `symbol_date` (`symbol_id`,`price_date`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


