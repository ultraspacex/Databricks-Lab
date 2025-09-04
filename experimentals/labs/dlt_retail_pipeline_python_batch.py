# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr, to_date, year, current_date, upper, trim, regexp_replace, when, lit, sha2, concat_ws, lower
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType

from pyspark.sql import functions as F

# COMMAND ----------

volumn_path = '/Volumes/catalog_yutthanal/default/d6_data'

# COMMAND ----------

# --- Bronze Customer Details (Batch Read) ---
@dlt.table(
        name="bronze_customer_details_batch",
        comment="Raw customer details ingested from CSV file using batch read.",
        table_properties={
                "quality": "bronze",
                "data_source": "customer_details_csv_batch"
        }
)
def bronze_customer_details_batch():
        schema = "CustomerID STRING, CustomerName STRING, EmailAddress STRING, PhoneNumber STRING, Address STRING, City STRING, State STRING, PostalCode STRING, Country STRING"
        return (
                spark.read
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load(f"{volumn_path}/raw_customer_details/customer_details.csv")
                .withColumn("input_file_name", lit(f"{volumn_path}/raw_customer_details/customer_details.csv"))
                .withColumn("processed_timestamp", expr("current_timestamp()"))
        )

# COMMAND ----------

# --- Bronze Product Catalog (Batch Read) ---
@dlt.table(
    name="bronze_product_catalog_batch",
    comment="Raw product catalog data ingested from CSV file using batch read.",
    table_properties={
        "quality": "bronze",
        "data_source": "product_catalog_csv"
    }
)
def bronze_product_catalog_batch():
    # Schema based on your provided column names for product_catalog.csv
    # Reading UnitPrice as DOUBLE directly. Spark will turn unparseable values into null.
    schema = "ProductID STRING, Item STRING, Category STRING, UnitPrice DOUBLE"
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{volumn_path}/raw_product_catalog/product_catalog.csv")
        .withColumn("input_file_name", F.lit(f"{volumn_path}/raw_product_catalog/product_catalog.csv"))
        .withColumn("processed_timestamp", F.expr("current_timestamp()"))
    )

# COMMAND ----------

# --- Bronze Sales Data (Batch Read) ---
@dlt.table(
    name="bronze_sales_batch",
    comment="Raw sales transaction data ingested from CSV file using batch read.",
    table_properties={
        "quality": "bronze",
        "data_source": "sales_csv" # 
    }
)
def bronze_sales_batch():
    # Schema based on your provided column names for sales.csv
    # Reading numeric fields (Quantity, UnitPrice, TaxAmount) as their target types directly.
    schema = """
        SalesOrderNumber STRING, SalesOrderLineNumber STRING, OrderDate STRING, 
        CustomerID STRING, Item STRING, 
        Quantity INTEGER, UnitPrice DOUBLE, TaxAmount DOUBLE
    """
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema) 
        .load(f"{volumn_path}/raw_sales/sales.csv")
        .withColumn("input_file_name", F.lit(f"{volumn_path}/raw_sales/sales.csv"))
        .withColumn("processed_timestamp", F.expr("current_timestamp()"))
    )


# COMMAND ----------

# --- Silver Customer Details ---
@dlt.table(
    name="silver_customer_details_batch",
    comment="Cleaned and conformed customer details with data quality checks (from batch source).",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("customer_id_not_null", "CustomerID IS NOT NULL") 
@dlt.expect("valid_email_format", "EmailAddress RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}' OR EmailAddress IS NULL") 
def silver_customer_details_batch():
    bronze_customers_df = dlt.read("bronze_customer_details_batch")
    
    customers_whitespace_trimmed_df = bronze_customers_df.withColumn(
        "CustomerID_whitespace_trimmed", 
        F.trim(F.col("CustomerID")) 
    )
    
    customers_final_id_df = customers_whitespace_trimmed_df.withColumn(
        "CustomerID_cleaned",
        F.when(
            F.col("CustomerID_whitespace_trimmed").isNotNull(),
            F.split(F.col("CustomerID_whitespace_trimmed"), "_")[0] 
        ).otherwise(F.col("CustomerID_whitespace_trimmed")) 
    )

    return (
        customers_final_id_df
        .withColumn("EmailAddress_cleaned", F.lower(F.trim(F.col("EmailAddress")))) 
        .withColumn("Country_cleaned", F.upper(F.trim(F.col("Country"))))          
        .withColumn("customer_sk", F.sha2(F.concat_ws("||", F.col("CustomerID_cleaned")), 256)) 
        .select(
            F.col("customer_sk"),
            F.col("CustomerID_cleaned").alias("CustomerID"), 
            F.col("CustomerName"),
            F.col("EmailAddress_cleaned").alias("EmailAddress"),
            F.col("PhoneNumber"), 
            F.col("Address"),    
            F.col("City"),        
            F.col("State"),       
            F.col("PostalCode"),  
            F.col("Country_cleaned").alias("Country"),
            F.col("input_file_name"),
            F.col("processed_timestamp")
        )
    )


# COMMAND ----------

# --- Silver Product Catalog ---
@dlt.table(
    name="silver_product_catalog_batch",
    comment="Cleaned and conformed product catalog with data quality checks (from batch source).",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("product_id_not_null", "ProductID IS NOT NULL") 
@dlt.expect("valid_item_name", "Item IS NOT NULL") 
@dlt.expect("valid_unit_price_catalog", "UnitPrice > 0 OR UnitPrice IS NULL") 
def silver_product_catalog_batch():
    bronze_products_df = dlt.read("bronze_product_catalog_batch")
    return (
        bronze_products_df
        .withColumn("ProductID_cleaned", F.trim(F.col("ProductID")))
        .withColumn("Item_cleaned", F.trim(F.col("Item")))
        .withColumn("Category_cleaned", F.trim(F.upper(F.col("Category"))))
        .select(
            F.col("ProductID_cleaned").alias("ProductID"),
            F.col("Item_cleaned").alias("Item"),
            F.col("Category_cleaned").alias("Category"),
            F.col("UnitPrice"), 
            F.col("input_file_name"),
            F.col("processed_timestamp")
        )
    )


# COMMAND ----------

# --- Silver Sales Data ---
@dlt.table(
    name="silver_sales_batch",
    comment="Cleaned and conformed sales transactions with data quality checks (from batch source).",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("sales_order_number_not_null", "SalesOrderNumber IS NOT NULL") 
@dlt.expect_or_drop("customer_id_not_null_sales", "CustomerID IS NOT NULL")       
@dlt.expect_or_drop("item_not_null_sales", "Item IS NOT NULL")                   
@dlt.expect("positive_quantity", "Quantity > 0 OR Quantity IS NULL") 
@dlt.expect("valid_unit_price_sales", "UnitPrice > 0 OR UnitPrice IS NULL") 
def silver_sales_batch():
    bronze_sales_df = dlt.read("bronze_sales_batch") 
    
    sales_whitespace_trimmed_df = bronze_sales_df.withColumn(
        "CustomerID_whitespace_trimmed",
        F.trim(F.col("CustomerID")) 
    )
    sales_final_id_df = sales_whitespace_trimmed_df.withColumn(
        "CustomerID_cleaned", 
        F.when(
            F.col("CustomerId_whitespace_trimmed").isNotNull(),
            F.split(F.col("CustomerId_whitespace_trimmed"), "_")[0]
        ).otherwise(F.col("CustomerId_whitespace_trimmed"))
    )

    return (
        sales_final_id_df 
        .withColumn("SalesOrderNumber_cleaned", F.trim(F.col("SalesOrderNumber")))
        .withColumn("Item_cleaned", F.trim(F.col("Item"))) 
        .withColumn("OrderDate_dt", F.to_date(F.col("OrderDate"), "MM/dd/yyyy"))
        .withColumn("TotalSaleAmount", F.expr("Quantity * UnitPrice + TaxAmount")) 
        .select(
            F.col("SalesOrderNumber_cleaned").alias("SalesOrderNumber"),
            F.col("SalesOrderLineNumber"),
            F.col("OrderDate_dt").alias("OrderDate"),
            F.col("CustomerID_cleaned").alias("CustomerID"), 
            F.col("Item_cleaned").alias("Item"),      
            F.col("Quantity"), 
            F.col("UnitPrice"), 
            F.col("TaxAmount"),
            F.col("TotalSaleAmount"),
            F.col("input_file_name"),
            F.col("processed_timestamp")
        )
    )


# COMMAND ----------

# --- Gold Customer Sales Summary ---
@dlt.table(
    name="gold_customer_sales_summary_batch",
    comment="Aggregated sales summary per customer and product category (from batch sources).",
    table_properties={"quality": "gold"}
)
def gold_customer_sales_summary_batch():
    sales_df = dlt.read("silver_sales_batch") 
    customers_df = dlt.read("silver_customer_details_batch") 
    products_df = dlt.read("silver_product_catalog_batch")

    joined_df = sales_df.join(customers_df, sales_df.CustomerID == customers_df.CustomerID, "inner") \
                        .join(products_df, sales_df.Item == products_df.Item, "inner")

    return (
        joined_df
        .groupBy(
            customers_df.customer_sk, 
            customers_df.CustomerID, 
            customers_df.CustomerName, 
            customers_df.EmailAddress, 
            customers_df.Country,      
            products_df.Category       
        )
        .agg(
            F.sum("TotalSaleAmount").alias("total_revenue_inc_tax"), 
            F.countDistinct("SalesOrderNumber").alias("total_unique_orders"), 
            F.avg("TotalSaleAmount").alias("average_order_value_inc_tax"),  
            F.sum("Quantity").alias("total_quantity_sold") 
        )
    )
