# Databricks notebook source
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dataquicks.customers_silver

# COMMAND ----------

df_customers = spark.sql("""select * from dataquicks.customers_silver;""")
display(df_customers.limit(10))

# COMMAND ----------

df_customers

# COMMAND ----------

def encryptData(cust_name):
    i =0 
    masked_name = ''
    for char_ in cust_name:
        if i%2 == 0:
            masked_name = masked_name + "*"
        else:
            masked_name = masked_name + char_
        i+=1
    return masked_name

encryptData('Romi Yadav')
udf_encryptData = udf(lambda x:encryptData(x), StringType())

# COMMAND ----------

df_customers = df_customers.withColumn("customer_name",udf_encryptData(col('customer_name')))
display(df_customers.limit(10))

# COMMAND ----------



# COMMAND ----------

