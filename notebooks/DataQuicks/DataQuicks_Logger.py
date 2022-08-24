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

import json
context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
context['tags']['hostName']

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
        Host_Id = context['tags']['hostName']
        if Start:
            Start_time = self.start_time
        if End:
            End_time = datetime.now()
        spark.sql("""
        insert into dataquicks.logs (Host_Id, Job_Name, Task_Name, Start_Time, End_Time, Status) 
        values ('{Host_Id}','{Job_Name}','{Task_Name}','{Start_time}','{End_time}','{Status}')
        """.format(Host_Id = Host_Id,
                   Job_Name = Job_Name,
                   Task_Name = Task_Name,
                   Start_time = Start_time,
                   End_time = End_time,
                   Status = Status))
        return None

# COMMAND ----------

# %sql
# Drop Table if exists dataquicks.logs; 
# Create Table dataquicks.logs (
#   Host_Id varchar(200),
#   Job_Name varchar(100),
#   Task_Name varchar(100),
#   Start_Time varchar(100),
#   End_Time varchar(100),
#   Status varchar(100)
# );

# COMMAND ----------


