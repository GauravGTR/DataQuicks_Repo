-- Databricks notebook source
-- MAGIC %run /DataQuicks/helper_functions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import yaml
-- MAGIC config = yaml.load(read_file_to_string('/dbfs/FileStore/DataQuicks/Universal_Widget.yml'), Loader=yaml.FullLoader)
-- MAGIC svoc_config = config['Data_Agg']
-- MAGIC svoc_config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC spark.conf.set('var.orders', svoc_config['orders'])
-- MAGIC spark.conf.set('var.products', svoc_config['products'])
-- MAGIC spark.conf.set('var.customers', svoc_config['customers'])
-- MAGIC spark.conf.set('var.run_date', svoc_config['run_date'])

-- COMMAND ----------

use dataquicks;

-- COMMAND ----------

drop table if exists product_daily_agg;
create table product_daily_agg 
using delta
as 
select date(order_datetime) as order_date,
		product_category, 
		product_name, 
		state, 
		count(distinct order_number) as cnt_orders,
		count(distinct a.customer_id) as cnt_customers,
		sum(qty) as sum_quantity,
		sum(price*qty) as sum_sales
from ${var.orders} as a
inner join 
(select distinct product_id ,product_category, product_name
from  products_bronze ) as b 
on  a.id = b.product_id 
inner join 
(select distinct customer_id ,state
from  ${var.customers} ) as c 
on  a.customer_id = c.customer_id
group by 1,2,3,4;

-- COMMAND ----------

drop table if exists category_daily_agg;
create table category_daily_agg 
using delta
as 
select date(order_datetime) as order_date,
		product_category,  
		count(distinct order_number) as cnt_orders,
		count(distinct a.customer_id) as cnt_customers,
		sum(qty) as sum_quantity,
		sum(price*qty) as sum_sales
from ${var.orders}  as a
inner join 
(select distinct product_id ,product_category, product_name
from  ${var.products} ) as b 
on  a.id = b.product_id 
group by 1,2;