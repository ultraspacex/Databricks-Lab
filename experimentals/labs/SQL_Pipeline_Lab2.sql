-- Databricks notebook source
-- Bronze layer: raw ingestion (ingest all raw data as-is)
CREATE OR REFRESH MATERIALIZED VIEW bronze_sales_raw
AS
SELECT *
FROM read_files(
  "/Volumes/catalog_yutthanal/default/d6_data/sales.csv",
  format => "csv",
  header => "true",
  inferSchema => "true"
);

-- Silver layer: clean and deduplicate, add data quality expectations
CREATE OR REFRESH MATERIALIZED VIEW silver_sales_cleaned (
  SalesOrderNumber STRING,
  SalesOrderLineNumber INT, 
  OrderDate DATE,
  CustomerId STRING,
  Item STRING,
  Quantity INT,
  UnitPrice DOUBLE,
  TaxAmount DOUBLE,
  
  -- Data quality expectations with violation handling
  CONSTRAINT order_id_not_null EXPECT (SalesOrderNumber IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT order_date_not_null EXPECT (OrderDate IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT quantity_valid EXPECT (Quantity BETWEEN 1 AND 100) ON VIOLATION DROP ROW,
  CONSTRAINT price_positive EXPECT (UnitPrice > 0) ON VIOLATION DROP ROW,
  CONSTRAINT tax_non_negative EXPECT (TaxAmount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT order_date_valid EXPECT (OrderDate <= CURRENT_DATE()) ON VIOLATION DROP ROW
)
AS
SELECT
  SalesOrderNumber,
  CAST(SalesOrderLineNumber AS INT) AS SalesOrderLineNumber,  -- Explicit cast to ensure type match
  to_date(OrderDate, 'dd-MM-yyyy') AS OrderDate,  -- Adjust format if dates differ
  CustomerId,
  Item,
  CAST(Quantity AS INT) AS Quantity,
  CAST(UnitPrice AS DOUBLE) AS UnitPrice,
  CAST(TaxAmount AS DOUBLE) AS TaxAmount
FROM LIVE.bronze_sales_raw;

-- Gold layer: aggregated sales data for reporting
CREATE OR REFRESH MATERIALIZED VIEW gold_sales_aggregated
AS
SELECT
  CustomerId,
  DATE_TRUNC('month', OrderDate) AS SalesMonth,
  COUNT(DISTINCT SalesOrderNumber) AS NumberOfOrders,
  SUM(Quantity) AS TotalQuantity,
  SUM(Quantity * UnitPrice) AS TotalSales,
  SUM(TaxAmount) AS TotalTax
FROM LIVE.silver_sales_cleaned
GROUP BY CustomerId, DATE_TRUNC('month', OrderDate);
