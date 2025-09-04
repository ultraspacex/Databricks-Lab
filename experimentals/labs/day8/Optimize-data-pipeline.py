# Databricks notebook source
import urllib.request

url = "https://github.com/parveenkrraina/essilor-batch02/raw/refs/heads/main/Day-08/yellow_tripdata_2021-01.parquet"
dbfs_path = "/dbfs/FileStore/data/yellow_tripdata_2021-01.parquet"

# Download the file to DBFS
dbutils.fs.cp(url, dbfs_path, recurse=True)

# COMMAND ----------

# Load the dataset into a DataFrame
df = spark.read.parquet("/dbfs/FileStore/data/yellow_tripdata_2021-01.parquet")
display(df)

# COMMAND ----------

df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/dbfs/stream_data/nyc_taxi_trips/schema/")
        .load("/dbfs/FileStore/"))
df.writeStream.format("delta") \
    .option("checkpointLocation", "/dbfs/stream_data/nyc_taxi_trips/checkpoints") \
    .option("mergeSchema", "true") \
    .start("/delta/nyc_taxi_trips")
display(df)

# COMMAND ----------

import urllib.request

url = "https://github.com/parveenkrraina/essilor-batch02/raw/refs/heads/main/Day-08/yellow_tripdata_2021-02_edited.parquet"
dbfs_path = "/nyc_taxi_trips/yellow_tripdata_2021-02_edited.parquet"

# Download the file to DBFS
dbutils.fs.cp(url, dbfs_path, recurse=True)

# COMMAND ----------

from pyspark.sql.functions import lit, rand

# Convert streaming DataFrame back to batch DataFrame
df = spark.read.parquet("/nyc_taxi_trips/*.parquet")

# Add a salt column
df_salted = df.withColumn("salt", (rand() * 100).cast("int"))

# Repartition based on the salted column
df_salted.repartition("salt").write.format("delta").mode("overwrite").save("/delta/nyc_taxi_trips_salted")

display(df_salted)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/delta/nyc_taxi_trips")
delta_table.optimize().executeCompaction()

# COMMAND ----------

delta_table.optimize().executeZOrderBy("tpep_pickup_datetime")
