# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
   name="raw_iot_data",
   comment="Raw IoT device data"
)
def raw_iot_data():
   return spark.readStream.format("delta").load("/tmp/delta/iot_data")

@dlt.table(
   name="transformed_iot_data",
   comment="Transformed IoT device data with derived metrics"
)
def transformed_iot_data():
   return (
       dlt.read("raw_iot_data")
       .withColumn("temperature_fahrenheit", col("temperature") * 9/5 + 32)
       .withColumn("humidity_percentage", col("humidity") * 100)
       .withColumn("event_time", current_timestamp())
   )
