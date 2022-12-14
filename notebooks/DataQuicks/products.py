# Databricks notebook source
# MAGIC % run 

# COMMAND ----------

from pyspark.sql.streaming import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

try:
    schema = 'product_id string,product_category string,product_name string,sales_price double,EAN13 bigint,EAN5 int,product_unit string'
    df = (spark
      .readStream
      .option("maxFilesPerTrigger", 5)
      .option("header", True)
      .schema(schema)
      .csv("dbfs:/dataquicks/raw_data/products/*/"))
    (df
    .withColumn("modified_dttm",F.current_timestamp())
     .writeStream
    .format("delta")
    .trigger(once=True)
    .outputMode("append")
    .option("checkpointLocation", "/dataquicks/checkpoints/products_bronze")
    .start("/dataquicks/tables/products_bronze"))


# COMMAND ----------

df_products = spark.read.format('delta').load('/dataquicks/tables/products_bronze')
df_res = (df_products.withColumn("rank",F.row_number().over(Window.partitionBy("product_id").orderBy(F.desc("modified_dttm"))))
    .where("rank==1").drop("rank")
     .where("product_id is not null and product_name is not null and product_name != ''"))
df_res.write.format('delta').mode('overWrite').save('/dataquicks/products_silver')
