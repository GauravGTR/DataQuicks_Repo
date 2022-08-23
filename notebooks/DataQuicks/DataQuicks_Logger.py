# Databricks notebook source
columns = ["Job_Name", 
           "Task_Name",
           "Start_Time",
           "End_Time",
           "Status"
          ]

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

class Logger:
    def __init__(self,):
        self.start_time = datetime.now()
        print(self.start_time)
        
    @staticmethod
    def start_logging():
        return datetime.now()
       
    def DataQuicks_Logger(self,Job_Name, Task_Name, Start = True, End = False, Status = "Running"):
        Start_time, End_time = None, None
        if Start:
            Start_time = self.start_time
        if End:
            End_time = datetime.now()
        spark.sql("""
        insert into dataquicks.logs (Job_Name, Task_Name, Start_Time, End_Time, Status) 
        values ('{Job_Name}','{Task_Name}','{Start_time}','{End_time}','{Status}')
        """.format(Job_Name = Job_Name,
                  Task_Name = Task_Name,
                  Start_time = Start_time,
                  End_time = End_time,
                  Status = Status))
        return None

# COMMAND ----------

# logger = Logger()

# COMMAND ----------

# logger.DataQuicks_Logger("test_job_2","test_task",End = True,Status = "Failed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dataquicks.logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop Table if exists dataquicks.logs; 
# MAGIC Create Table dataquicks.logs (
# MAGIC   Job_Name varchar(100),
# MAGIC   Task_Name varchar(100),
# MAGIC   Start_Time varchar(100),
# MAGIC   End_Time varchar(100),
# MAGIC   Status varchar(100)
# MAGIC );

# COMMAND ----------


