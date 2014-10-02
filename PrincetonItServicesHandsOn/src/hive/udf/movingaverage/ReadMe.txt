**  Hive tables are stored under warehouse directory as file.
user/
hive/
warehouse/
stocks

CREATE TABLE IF NOT EXISTS stocks (
ymd               STRING,
price_open        FLOAT,
price_high        FLOAT,
price_low         FLOAT,
price_close       FLOAT,
volume            INT,
price_adj_close   FLOAT
)
PARTITIONED BY (exchange STRING, symbol STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

** Partition tables are always present at the end of the table. This was approach provided by Hive so if file format is different create dummy table and then import
---------------------------------------------------------------------------------------------------------------------------------
#We don't have any partitions yet:

SHOW PARTITIONS stocks;
---------------------------------------------------------------------------------------------------------------------------------

#For EXTERNAL, partitioned tables, you use ALTER TABLE to add each
# partition and specify a unique directory for its data.
# We'll just add data for four stocks:

ALTER TABLE stocks ADD PARTITION(exchange = 'NYSE', symbol = 'AEA')


ALTER TABLE stocks ADD PARTITION(exchange = 'NYSE', symbol = 'GEO')


ALTER TABLE stocks ADD PARTITION(exchange = 'NYSE', symbol = 'IBM')
-----------------------------------------------------------------------------------------------------------------------------------

SHOW PARTITIONS stocks;

--------------------------------------Creating External tables so only the meta data is lost when deleted--------------------------

# Creating table2 and then importing it into original table.
CREATE TABLE IF NOT EXISTS stocks_external (
exchange STRING, symbol STRING,
ymd               STRING,
price_open        FLOAT,
price_high        FLOAT,
price_low         FLOAT,
price_close       FLOAT,
volume            INT,
price_adj_close   FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

--------------------------------------Import Data----------------------------------------------------------------------------------

Upload 3 files from Hue(File Browser) to /user/training hdfs path 

NYSE_daily_prices_A.csv
NYSE_daily_prices_I.csv
NYSE_daily_prices_G.csv

LOAD DATA INPATH '/user/training/NYSE_daily_prices_A.csv' INTO TABLE stocks_external
LOAD DATA INPATH '/user/training/NYSE_daily_prices_I.csv' INTO TABLE stocks_external 
LOAD DATA INPATH '/user/training/NYSE_daily_prices_G.csv' INTO TABLE stocks_external 

--------------------------------------Import Data into Original Stocks Table-------------------------------------------------------
INSERT INTO table stocks
PARTITION (exchange = 'NYSE', symbol = 'IBM')
SELECT  ymd,price_open,price_high,price_low,price_close,volume,price_adj_close
FROM    stocks_external WHERE  exchange = 'NYSE' and symbol = 'IBM'

INSERT INTO table stocks
PARTITION (exchange = 'NYSE', symbol = 'AEA')
SELECT  ymd,price_open,price_high,price_low,price_close,volume,price_adj_close
FROM    stocks_external WHERE  exchange = 'NYSE' and symbol = 'AEA'

INSERT INTO table stocks
PARTITION (exchange = 'NYSE', symbol = 'GEO')
SELECT  ymd,price_open,price_high,price_low,price_close,volume,price_adj_close
FROM    stocks_external WHERE  exchange = 'NYSE' and symbol = 'GEO'


---------------------------------------Steps to execute it----------------------------------------------------------------------------

Step 3:  Run the following HQL script (custom_udf.hql) to compute moving average of stock prices.

[training@localhost ~]$ /usr/bin/hive -f custom_udf.hql

-- Custom UDFs
CREATE DATABASE IF NOT EXISTS training;

USE training;

-- We'll use a Custom UDF implemented in Java to calculate the moving
-- average of stocks.

-- Built this java code. The "jar" that you incorporate using
-- the ADD JAR command is named "moving-avg-udf.jar". Let's now add this jar.
-- Note the relative path to the jar used below; we assume you started the
-- CLI in this directory, that is, the directory that holds this HQL script!

--This command will add the "jar" file into HIVE library.
--NOTE: Exclamatory symbol (!) placed before any command
-- will treat the command to be executed from terminal instead of HIVE shell

!sudo cp /home/training/moving-avg-udf.jar /usr/lib/hive/lib

ADD JAR moving-avg-udf.jar;

CREATE TEMPORARY FUNCTION moving_avg AS 'com.training.hiveudfs.MovingAverageUDF';

-- Verify it appears in...

SHOW FUNCTIONS;

DESCRIBE FUNCTION EXTENDED moving_avg;

SELECT ymd, symbol, price_close, moving_avg(50, symbol, price_close)
FROM stocks
WHERE symbol = 'AAPL' LIMIT 20;

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/apple_ibm'
SELECT ymd, symbol, price_close, moving_avg(50, symbol, price_close)
FROM stocks
WHERE symbol = 'AAPL' OR symbol = 'IBM';

-- When you're done with the directory, you can delete it.

You have successfully implemented a custom UDF in HIVE!