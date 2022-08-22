# Databricks notebook source
# MAGIC %sql
# MAGIC -- create view dataquicks.orders_rank as 
# MAGIC -- select order_date,customer_id,customer_name,number_of_line_items,order_datetime,order_number,curr,id,name,price,qty,unit,promo_disc,promo_id,promo_item,promo_qty,modified_dttm,row_number() over(partition by order_number,order_datetime,number_of_line_items order by order_date desc) as rnk from dataquicks.orders_silver where modified_dttm > (select max(modified_dttm) from dataquicks.orders)

# COMMAND ----------


display(spark.sql("""merge into dataquicks.orders tgt
using (select order_date,customer_id,customer_name,number_of_line_items,order_datetime,order_number,curr,id,name,price,qty,unit,promo_disc,promo_id,promo_item,promo_qty,modified_dttm from dataquicks.orders_rank where rnk=1) as src
on tgt.order_number=src.order_number 
and tgt.order_datetime=src.order_datetime 
and tgt.number_of_line_items=src.number_of_line_items
when matched then update set
order_date=src.order_date,
customer_id=src.customer_id,
customer_name=src.customer_name,
number_of_line_items=src.number_of_line_items,
order_datetime=src.order_datetime,
order_number=src.order_number,
curr=src.curr,
id=src.id,
name=src.name,
price=src.price,
qty=src.qty,
unit=src.unit,
promo_disc=src.promo_disc,
promo_id=src.promo_id,
promo_item=src.promo_item,
promo_qty=src.promo_qty,
modified_dttm=src.modified_dttm
when not matched then insert (
order_date,customer_id,customer_name,number_of_line_items,order_datetime,order_number,curr,id,name,price,qty,unit,promo_disc,promo_id,promo_item,promo_qty,modified_dttm)
values (
order_date,customer_id,customer_name,number_of_line_items,order_datetime,order_number,curr,id,name,price,qty,unit,promo_disc,promo_id,promo_item,promo_qty,modified_dttm)""")
       )