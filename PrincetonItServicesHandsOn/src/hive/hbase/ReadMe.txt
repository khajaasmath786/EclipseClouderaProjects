http://princetonits.com/technology/integrate-hive-with-hbase-and-query-using-impala/

-------------------------Integrating Hive with Hbase  ---------------------------------------------------

HBase tables can be integrated with HIVE, so that querying can be done using IMPALA. IMPALA queries are pretty fast and as easy as any standard SQL queries. We shall load transactional data into HBase table integrated with HIVE using ImportTSV method, and then query the corresponding HIVE table from IMPALA.

The transactional data set has the following fields:

txtno, ymd, custno, amount, category, product, city, state, spentby

00000000,06-26-2011,4007024,040.33,Exercise & Fitness,Cardio Machine Accessories,Clarksville,Tennessee,credit
00000001,05-26-2011,4006742,198.44,Exercise & Fitness,Weightlifting Gloves,Long Beach,California,credit
00000002,06-01-2011,4009775,005.58,Exercise & Fitness,Weightlifting Machine Accessories,Anaheim,California,credit
00000003,06-05-2011,4002199,198.19,Gymnastics,Gymnastics Rings,Milwaukee,Wisconsin,credit
00000004,12-17-2011,4002613,098.81,Team Sports,Field Hockey,Nashville  ,Tennessee,credit
00000005,02-14-2011,4007591,193.63,Outdoor Recreation,Camping & Backpacking & Hiking,Chicago,Illinois,credit
00000006,10-28-2011,4002190,027.89,Puzzles,Jigsaw Puzzles,Charleston,South Carolina,credit
00000007,07-14-2011,4002964,096.01,Outdoor Play Equipment,Sandboxes,Columbus,Ohio,credit
00000008,01-17-2011,4007361,010.44,Winter Sports,Snowmobiling,Des Moines,Iowa,credit
00000009,05-17-2011,4004798,152.46,Jumping,Bungee Jumping,St. Petersburg,Florida,credit
00000010,05-29-2011,4004646,180.28,Outdoor Recreation,Archery,Reno,Nevada,credit
00000011,06-18-2011,4008071,121.39,Outdoor Play Equipment,Swing Sets,Columbus,Ohio,credit
00000012,02-08-2011,4002473,041.52,Indoor Games,Bowling,San Francisco,California,credit
00000013,03-13-2011,4003268,107.80,Team Sports,Field Hockey,Honolulu  ,Hawaii,credit
00000014,02-25-2011,4004613,036.81,Gymnastics,Vaulting Horses,Los Angeles,California,credit
---------------------------------------------------------------------------------------------------------
Step 1: Create a table in HIVE and map it to HBase using org.apache.hadoop.hive.hbase.HBaseStorageHandler property.Note that the name of the HIVE table is different from that of HBase table for convenience. Here ‘transactions’ is a HIVE table and is mapped with the HBase table ‘transactions_hbase’.

CREATE TABLE transactions(
txtno int,
ymd string,
custno int,
amount float,
category string,
product string,
city string,
state string,
spentby string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES 
("hbase.columns.mapping"="
:key,details:ymd,details:custno,details:amount,details:category,details:product,details:city,details:state,details:spentby") 
TBLPROPERTIES ("hbase.table.name"="transactions_hbase");
---------------------------------------------------------------------------------------------------------
Step 2: Verify whether the table(s) is created in HIVE and HBase from their corresponding shells.

[training@localhost ~]$ hbase shell
hbase(main):001:0> list
hbase(main):001:1> describe 'transactions_hbase'
hbase(main):001:2> exit

[training@localhost ~]$ hive
hive> show tables;
hive> describe extended transactions;
hive> quit;
---------------------------------------------------------------------------------------------------------
Step 3: Load the transactions data from HDFS into the HBase table created i.e transactions_hbase, using ImportTSV method. You may also load this data into HBase table using a pig script. To load using PIG script refer to this post. Note that the data gets automatically loaded into HIVE table which was integrated with this HBase table.

[training@localhost ~]$ hbase org.apache.hadoop.hbase.mapreduce.ImportTsv '-Dimporttsv.separator=,' -Dimporttsv.columns=HBASE_ROW_KEY,details:ymd,details:custno,details:amount,details:category,details:product,details:city,details:state,details:spentby transactions_hbase /user/training/transactions_data.txt;
---------------------------------------------------------------------------------------------------------
Step 4: Check whether the data got properly loaded into both HIVE and HBase tables.

[training@localhost ~]$ hbase shell
hbase(main):001:0> scan 'transactions_hbase'
hbase(main):001:2> exit

[training@localhost ~]$ hive
hive> select * from transactions limit 10;
hive> quit;
---------------------------------------------------------------------------------------------------------
Step 5: Now lets try to access this HIVE table (transactions) from IMAPLA shell. Note that invalidate metadata command is needed to refresh the metadata and to make sure all the available HIVE tables reflect in IMPALA.

[training@localhost ~]$ impala-shell
[localhost.localdomain:21000] > invalidate metadata;
[localhost.localdomain:21000] > select * from transactions limit 5;

You can do as many queries as you may like and compare the time efficiency between HIVE, HBase and IMPALA.
