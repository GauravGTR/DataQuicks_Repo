# Databricks notebook source
# MAGIC %pip install logger
# MAGIC %pip install lxml

# COMMAND ----------

# DBTITLE 1,Imports
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time
# from logger import log_info
import logging
logging.basicConfig(filename='/dbfs/dataquicks/logging/dataquicks'+time.strftime("%Y%m%d%H%M%S")+'.log', 
                    encoding='utf-8',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

# COMMAND ----------

@dlt.create_table(
  comment = 'sales orders raw data',
  partition_cols = ["order_date"])
def orders_bronze():
    logging.info('started orders bronze')
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("badRecordsPath",'dbfs:/dataquicks/quarantine/orders_bronze')
            .option("checkpointLocation", "dbfs:/dataquicks/checkpoint/orders_bronze")
            .option("cloudFiles.inferColumnTypes", True)
            .load("dbfs:/dataquicks/raw_data/orders/*/")
            .withColumn("order_date",F.to_date(F.from_unixtime('order_datetime')))
    )

# COMMAND ----------

@dlt.table(
  comment="sales order data cleaned and prepared for analysis.",
  partition_cols = ["order_date"]
)
@dlt.expect_or_drop("valid_order_number", "order_number IS NOT NULL")
@dlt.expect_or_drop("valid_order_datetime", "order_datetime IS NOT NULL and order_datetime != ''")
def orders_silver():
  return (
    dlt.read_stream("orders_bronze")
        .select(*['order_date','customer_id', 'customer_name', 'number_of_line_items', 'order_datetime', 'order_number'],F.explode('ordered_products').alias('ordered_products'))
        .select(*['order_date','customer_id', 'customer_name', 'number_of_line_items', 'order_datetime', 'order_number'],'ordered_products.*')
        .select(*['order_date','customer_id', 'customer_name', 'number_of_line_items', 'order_datetime', 'order_number', 'curr', 'id', 'name', 'price', 'qty', 'unit'],'promotion_info.*')
        .withColumn("order_datetime",F.from_unixtime('order_datetime'))
        .withColumn("modified_dttm",F.current_timestamp())
  )

# COMMAND ----------

@dlt.table(
  comment="bad records"
)
@dlt.expect_or_drop("valid_order_number", "order_number IS NULL")
@dlt.expect_or_drop("valid_order_datetime", "order_datetime IS NULL and order_datetime = ''")
def orders_bad_records():
  return (
    dlt.read_stream("orders_bronze")
        .select(*['order_date','customer_id', 'customer_name', 'number_of_line_items', 'order_datetime', 'order_number'],F.explode('ordered_products').alias('ordered_products'))
        .select(*['order_date','customer_id', 'customer_name', 'number_of_line_items', 'order_datetime', 'order_number'],'ordered_products.*')
        .select(*['order_date','customer_id', 'customer_name', 'number_of_line_items', 'order_datetime', 'order_number', 'curr', 'id', 'name', 'price', 'qty', 'unit'],'promotion_info.*')
        .withColumn("order_datetime",F.from_unixtime('order_datetime'))
        .withColumn("modified_dttm",F.current_timestamp())
  )

# COMMAND ----------

@dlt.create_table
def customers_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            .option("badRecordsPath",'dbfs:/dataquicks/quarantine/customers')
            .option("checkpointLocation", "/dataquicks/checkpoint/customers")
            .option("cloudFiles.inferColumnTypes", True)
            .load("dbfs:/dataquicks/raw_data/customers/*/"))

# COMMAND ----------

dtypes_cust = [('customer_id', 'int'), ('tax_id', 'double'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'double'), ('lat', 'double'), ('ship_to_address', 'string'), ('valid_from', 'int'), ('valid_to', 'double'), ('units_purchased', 'double'), ('loyalty_segment', 'int')]
@dlt.table
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_name", "customer_name IS NOT NULL and customer_name != ''")

def customers_silver():
  return (
    dlt.read("customers_bronze")
    .select(*(F.col(c[0]).cast(c[1]).alias(c[0]) for c in dtypes_cust))
    .withColumn("valid_from",F.from_unixtime('valid_from'))
    .withColumn("valid_to",F.from_unixtime('valid_to'))
    .withColumn("rank",F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("valid_to"))))
    .where("rank==1").drop("rank")
  )

# COMMAND ----------


@dlt.create_table
def active_promotions_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("checkpointLocation", "/dataquicks/checkpoint/active_promotions")
            .option("cloudFiles.inferColumnTypes", False)
            .schema("promo_customer string,promo_item string,promo_disc double,promo_id bigint,promo_datetime string,promo_qty bigint,cumsum bigint,promo_began string,units_required bigint,eligible bigint,deadline string")
            .load("dbfs:/dataquicks/raw_data/active_promotions/*/"))

# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("valid_promo_id", "promo_id IS NOT NULL")
@dlt.expect_or_drop("valid_promo_datetime", "promo_datetime IS NOT NULL and promo_datetime != ''")
def active_promotions_silver():
  return (
    dlt.read("active_promotions_bronze")
    .withColumn("promo_datetime",F.from_unixtime('promo_datetime'))
    .withColumn("rank",F.row_number().over(Window.partitionBy("promo_datetime","promo_customer","promo_item").orderBy(F.desc("promo_datetime"))))
    .where("rank==1").drop("rank")
  )

# COMMAND ----------

# @dlt.create_table
# def suppliers_bronze():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "csv")
#             .option("header", True)
#             .option("badRecordsPath",'dbfs:/dataquicks/quarantine/suppliers')
#             .option("cloudFiles.inferColumnTypes", True)
#             .load("dbfs:/dataquicks/raw_data/suppliers"))


# COMMAND ----------

# dtypes_suppliers = [('EAN13', 'int'),('EAN5', 'int'),('PO', 'int'),('brand', 'string'),('datetime', 'int'),('price', 'string'),('product_name', 'string'), 	('product_unit', 'string'),('purchaser', 'string'),('quantity', 'int'),('supplier', 'string'),('password_hash', 'string'),('missing_price', 'boolean'), ('banned_supplier', 'boolean'),('warehouse_alert', 'string') ]
# @dlt.table
# @dlt.expect_or_drop("valid_EAN13", "EAN13 IS NOT NULL")
# @dlt.expect_or_drop("valid_EAN5", "EAN5 IS NOT NULL ")
# def suppliers_silver():
#   return (
#     dlt.read("suppliers_bronze")
#     .select(*(col(c[0]).cast(c[1]).alias(c[0]) for c in dtypes_suppliers))
#     .withColumn("datetime",from_unixtime('valid_from'))
    
#   )