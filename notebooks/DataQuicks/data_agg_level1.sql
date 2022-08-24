-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Helper functions

-- COMMAND ----------

-- MAGIC %run /DataQuicks/helper_functions

-- COMMAND ----------

-- MAGIC %run ./DataQuicks_Logger

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # dbutils.fs.rm('dbfs:/FileStore/DataQuicks/Universal_Widget.yml',True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #logger object this gets refreshed & new object will be created whenever we run a new job.
-- MAGIC logger = Logger()
-- MAGIC logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_1",End = False,Status = "Started")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import yaml
-- MAGIC config = yaml.load(read_file_to_string('/dbfs/FileStore/DataQuicks/Universal_Widget.yml'), Loader=yaml.FullLoader)
-- MAGIC svoc_config = config['Data_Agg']
-- MAGIC svoc_config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reading the notebook parameters

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC spark.conf.set('var.orders', svoc_config['orders'])
-- MAGIC spark.conf.set('var.products', svoc_config['products'])
-- MAGIC spark.conf.set('var.customers', svoc_config['customers'])
-- MAGIC spark.conf.set('var.run_date', svoc_config['run_date'])
-- MAGIC spark.conf.set('var.finalDB', svoc_config['finalDB'])

-- COMMAND ----------

use ${var.finalDB}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data agg Level 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_1",End = False,Status = "Running")
-- MAGIC     spark.sql("""drop table if exists customer_daily_agg;""")
-- MAGIC     spark.sql("""create table customer_daily_agg 
-- MAGIC     using delta
-- MAGIC     as 
-- MAGIC     select  customer_id,
-- MAGIC             date(order_datetime) as order_date,
-- MAGIC             count(distinct order_number) as cnt_orders,
-- MAGIC             sum(qty) as sum_quantity,
-- MAGIC             sum(price*qty) as sum_sales
-- MAGIC     from ${var.orders} 
-- MAGIC     group by 1,2
-- MAGIC     ;""")
-- MAGIC     fail
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_1",End = True,Status = "Successful")
-- MAGIC except Exception as e:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_1",End = True,Status = "Failed")
