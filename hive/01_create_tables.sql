CREATE DATABASE IF NOT EXISTS banking;
USE banking;
CREATE EXTERNAL TABLE IF NOT EXISTS bank_marketing (
  age INT, job STRING, marital STRING, education STRING, `default` STRING,
  balance BIGINT, housing STRING, loan STRING, contact STRING, day INT,
  month STRING, duration INT, campaign INT, pdays INT, previous INT,
  poutcome STRING, y STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar"=",", "quoteChar"="\"", "escapeChar"="\\")
STORED AS TEXTFILE
LOCATION 'hdfs:///data/banking/raw/'
TBLPROPERTIES ("skip.header.line.count"="1");
