import argparse, os
from pyspark.sql import SparkSession, functions as F, types as T
p=argparse.ArgumentParser()
p.add_argument("--input",default="streaming/incoming")
p.add_argument("--output",default="outputs/streaming/alerts")
p.add_argument("--checkpoint",default="outputs/checkpoints/alerts")
args=p.parse_args()
schema=T.StructType([
 T.StructField("transaction_id",T.StringType()),T.StructField("account_id",T.StringType()),
 T.StructField("event_time",T.StringType()),T.StructField("amount",T.DoubleType()),
 T.StructField("merchant_category",T.StringType()),T.StructField("channel",T.StringType()),
 T.StructField("country",T.StringType())])
spark=SparkSession.builder.appName("BankingStructuredStreaming").getOrCreate()
events=(spark.readStream.schema(schema).option("maxFilesPerTrigger",1).json(args.input)
 .withColumn("event_ts",F.to_timestamp("event_time")).withWatermark("event_ts","2 minutes"))
alerts=(events.withColumn("alert_reason",F.when(F.col("amount")>=2500,"HIGH_AMOUNT")
 .when((F.col("country")!="IN")&(F.col("amount")>=500),"FOREIGN_HIGH_AMOUNT"))
 .filter(F.col("alert_reason").isNotNull()))
q=(alerts.writeStream.format("json").outputMode("append").option("path",args.output)
 .option("checkpointLocation",args.checkpoint).trigger(processingTime="5 seconds").start())
summary=(events.groupBy(F.window("event_ts","1 minute"),"channel")
 .agg(F.count("*").alias("transaction_count"),F.round(F.sum("amount"),2).alias("total_amount")))
q2=(summary.writeStream.format("console").outputMode("append")
 .option("checkpointLocation",args.checkpoint+"_summary").trigger(processingTime="5 seconds").start())
try:
    spark.streams.awaitAnyTermination()
finally:
    for query in spark.streams.active: query.stop()
    spark.stop()
