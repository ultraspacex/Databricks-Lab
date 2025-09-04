# Databricks notebook source
# Set the catalog name
catalog = 'catalog_yutthanal' 

# Set variable
dbutils.widgets.text("catalog", catalog)  
catalog = dbutils.widgets.get("catalog") 

# COMMAND ----------

# MAGIC %md
# MAGIC Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalog} 
# MAGIC   COMMENT '${catalog} Catalog for governance lab';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Upload test file to DBFS**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.legacy_db;
# MAGIC

# COMMAND ----------

# Declare CSV PATH
csv_path = f'/Volumes/{catalog}/default/crm'

dbutils.widgets.text("csv_path", csv_path)
csv_path = dbutils.widgets.get("csv_path") 

print(csv_path)

# COMMAND ----------

# sales.csv
df_sales = spark.read.csv(f'{csv_path}/sales.csv', header=True, inferSchema=True)
df_sales.write.mode('overwrite').saveAsTable(f'{catalog}.legacy_db.sales')

# customer_details.csv
df_cust = spark.read.csv(f'{csv_path}/customer_details.csv', header=True, inferSchema=True)
df_cust.write.mode('overwrite').saveAsTable(f'{catalog}.legacy_db.customer_details')

# product_catalog.csv
df_prod = spark.read.csv(f'{csv_path}/product_catalog.csv', header=True, inferSchema=True)
df_prod.write.mode('overwrite').saveAsTable(f'{catalog}.legacy_db.product_catalog')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.legacy_db.sales LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.raw;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.raw.sales AS
# MAGIC SELECT * FROM ${catalog}.legacy_db.sales;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.raw.customer_details AS
# MAGIC SELECT * FROM ${catalog}.legacy_db.customer_details;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.raw.product_catalog AS
# MAGIC SELECT * FROM ${catalog}.legacy_db.product_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.raw.sales LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES --IN ${catalog};

# COMMAND ----------

# Optional: List legacy tables
display(spark.sql(f'SHOW TABLES IN {catalog}.default'))

# COMMAND ----------

# MAGIC %md
# MAGIC Need to create a privillage in Administrator.

# COMMAND ----------

# Declare Privillage group name
group_role = 'data_engineers'
user_role = 'analysts'

# masked data user role
user_role_masked = 'account users'

dbutils.widgets.text("group_role", group_role)
dbutils.widgets.text("user_role", user_role)
dbutils.widgets.text("user_role_masked", user_role_masked)

group_role = dbutils.widgets.get("group_role") 
user_role = dbutils.widgets.get("user_role")
user_role_masked = dbutils.widgets.get("user_role_masked")  

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Grant catalog and schema usage to a group (replace `data_engineers` with your real group)*/
# MAGIC GRANT USAGE ON CATALOG ${catalog} TO ${group_role};
# MAGIC GRANT USAGE ON SCHEMA ${catalog}.raw TO ${user_role};
# MAGIC
# MAGIC GRANT SELECT ON TABLE ${catalog}.raw.sales TO ${user_role};
# MAGIC GRANT SELECT ON TABLE ${catalog}.raw.customer_details TO ${user_role};
# MAGIC GRANT SELECT ON TABLE ${catalog}.raw.product_catalog TO ${user_role};
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${catalog}.raw.customer_details_masked AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE WHEN is_member('admins') THEN EmailAddress ELSE '***MASKED***' END AS masked_email
# MAGIC FROM ${catalog}.raw.customer_details; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC GRANT SELECT ON VIEW ${catalog}.raw.customer_details_masked TO `${user_role_masked}`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ${catalog}.raw.sales SET TBLPROPERTIES (
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 30 days'
# MAGIC );
# MAGIC
# MAGIC VACUUM ${catalog}.raw.sales RETAIN 720 HOURS;
# MAGIC

# COMMAND ----------

# Query as analyst (or test with different users)
display(spark.sql('SELECT * FROM ${catalog}.raw.sales LIMIT 5'))

# COMMAND ----------

# MAGIC %md
# MAGIC # This part is will be romove all Schema & Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS ${catalog}.raw.customer_details_masked;
# MAGIC DROP TABLE IF EXISTS ${catalog}.raw.sales;
# MAGIC DROP TABLE IF EXISTS ${catalog}.raw.customer_details;
# MAGIC DROP TABLE IF EXISTS ${catalog}.raw.product_catalog;
# MAGIC DROP SCHEMA IF EXISTS ${catalog}.raw CASCADE;
# MAGIC DROP CATALOG IF EXISTS ${catalog} CASCADE;
