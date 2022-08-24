-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Helper functions

-- COMMAND ----------

-- MAGIC %run /DataQuicks/helper_functions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # dbutils.fs.rm('dbfs:/FileStore/DataQuicks/Universal_Widget.yml',True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
-- MAGIC print(context)

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

drop table if exists customer_daily_agg;
create table customer_daily_agg 
using delta
as 
select  customer_id,
        date(order_datetime) as order_date,
		count(distinct order_number) as cnt_orders,
		sum(qty) as sum_quantity,
		sum(price*qty) as sum_sales
from ${var.orders} 
group by 1,2
;
