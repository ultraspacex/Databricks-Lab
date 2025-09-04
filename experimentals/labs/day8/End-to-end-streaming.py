# Databricks notebook source
import urllib.request

url = "https://raw.githubusercontent.com/parveenkrraina/essilor-batch02/refs/heads/main/Day-08/device_data.csv"
dbfs_path = "dbfs:/FileStore/data/device_data.csv"

# Download the file to DBFS
dbutils.fs.cp(url, dbfs_path, recurse=True)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the schema for the incoming data
schema = StructType([
   StructField("device_id", StringType(), True),
   StructField("timestamp", TimestampType(), True),
   StructField("temperature", DoubleType(), True),
   StructField("humidity", DoubleType(), True)
])

# Read streaming data from folder
inputPath = 'dbfs:/FileStore/data/'
iotstream = spark.readStream.schema(schema).option("header", "true").csv(inputPath)
print("Source stream created...")

# Write the data to a Delta table
query = (iotstream
        .writeStream
        .format("delta")
        .option("checkpointLocation", "/tmp/checkpoints/iot_data")
        .start("/tmp/delta/iot_data"))

# COMMAND ----------

query.stop()
