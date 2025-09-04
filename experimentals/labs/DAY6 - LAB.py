# Databricks notebook source
# Set the catalog name
catalog = 'catalog_yutthanal' 

# Set variable
dbutils.widgets.text("catalog", catalog)  
catalog = dbutils.widgets.get("catalog") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.sales.bronze_sales LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.sales.silver_sales LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.sales.gold_sales_summary LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS ${catalog}.sales_lab3 CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze raw data
# MAGIC SELECT * FROM ${catalog}.sales.bronze_sales_raw LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver cleaned data
# MAGIC SELECT * FROM ${catalog}.sales.silver_sales_cleaned LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold aggregated data
# MAGIC SELECT * FROM ${catalog}.sales.gold_sales_aggregated ORDER BY order_date DESC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, length(CustomerID) as id_length FROM ${catalog}.sales_lab3.silver_customer_details_batch LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, length(CustomerID) as id_length FROM ${catalog}.sales_lab3.silver_sales_batch LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, customer_sk, CustomerName, total_revenue_inc_tax FROM ${catalog}.sales_lab3.gold_customer_sales_summary_batch ORDER BY total_revenue_inc_tax DESC LIMIT 10;
