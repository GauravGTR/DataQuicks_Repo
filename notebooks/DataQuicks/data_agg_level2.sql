-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Helper functions

-- COMMAND ----------

-- MAGIC %run /DataQuicks/helper_functions

-- COMMAND ----------

-- MAGIC %run ./DataQuicks_Logger

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #logger object this gets refreshed & new object will be created whenever we run a new job.
-- MAGIC logger = Logger()
-- MAGIC logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = False,Status = "Started")

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

use ${var.finalDB};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Product Data agg Level 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = False,Status = "Running")
-- MAGIC     spark.sql("""drop table if exists product_daily_agg;""")
-- MAGIC     spark.sql("""create table product_daily_agg 
-- MAGIC     using delta
-- MAGIC     as 
-- MAGIC     select date(order_datetime) as order_date,
-- MAGIC           product_category, 
-- MAGIC           product_name, 
-- MAGIC           state, 
-- MAGIC           count(distinct order_number) as cnt_orders,
-- MAGIC           count(distinct a.customer_id) as cnt_customers,
-- MAGIC           sum(qty) as sum_quantity,
-- MAGIC           sum(price*qty) as sum_sales
-- MAGIC     from ${var.orders} as a
-- MAGIC     inner join 
-- MAGIC     (select distinct product_id ,product_category, product_name
-- MAGIC     from  ${var.products} ) as b 
-- MAGIC     on  a.id = b.product_id 
-- MAGIC     inner join 
-- MAGIC     (select distinct customer_id ,state
-- MAGIC     from  ${var.customers} ) as c 
-- MAGIC     on  a.customer_id = c.customer_id
-- MAGIC     group by 1,2,3,4;""")
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = True,Status = "Successful")
-- MAGIC except Exception as e:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = True,Status = "Failed")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Category Data agg Level 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = False,Status = "Running")
-- MAGIC     spark.sql("""drop table if exists category_daily_agg;""")
-- MAGIC     spark.sql("""create table category_daily_agg 
-- MAGIC     using delta
-- MAGIC     as 
-- MAGIC     select date(order_datetime) as order_date,
-- MAGIC             product_category,  
-- MAGIC             count(distinct order_number) as cnt_orders,
-- MAGIC             count(distinct a.customer_id) as cnt_customers,
-- MAGIC             sum(qty) as sum_quantity,
-- MAGIC             sum(price*qty) as sum_sales
-- MAGIC     from ${var.orders}  as a
-- MAGIC     inner join 
-- MAGIC     (select distinct product_id ,product_category, product_name
-- MAGIC     from  ${var.products} ) as b 
-- MAGIC     on  a.id = b.product_id 
-- MAGIC     group by 1,2;""")
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = True,Status = "Successful")
-- MAGIC except Exception as e:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = True,Status = "Failed")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer Analytical Record (CAR)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = False,Status = "Running")
-- MAGIC     spark.sql("""drop table if exists CAR;""")
-- MAGIC     spark.sql("""create table CAR 
-- MAGIC     using delta
-- MAGIC     as 
-- MAGIC     select  customer_id,
-- MAGIC             min(order_date) as first_txn_dt,
-- MAGIC             max(order_date) as last_txn_dt,
-- MAGIC             sum(cnt_orders) as cnt_orders,
-- MAGIC             sum(sum_quantity) as sum_quantity,
-- MAGIC             sum(sum_sales) as sum_sales,
-- MAGIC             sum(sum_sales)/sum(cnt_orders) as sales_per_order,
-- MAGIC             sum(sum_quantity)/sum(cnt_orders) as qty_per_order
-- MAGIC     from customer_daily_agg
-- MAGIC     group by 1
-- MAGIC     ;""")
-- MAGIC except Exception as e:
-- MAGIC     logger.DataQuicks_Logger("Data_Aggregation_Workflow","Level_2",End = True,Status = "Failed")