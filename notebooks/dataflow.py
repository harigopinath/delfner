# Databricks notebook source
# MAGIC %md
# MAGIC # Data pipeline example
# MAGIC #### Sample notebook that defines a dataflow using the delfner library

# COMMAND ----------

# DBTITLE 1,Import the class ArchiveFiles from library delfner
from com.networkrail.delfner.main.Archival import ArchiveFiles

# COMMAND ----------

# DBTITLE 1,Instantiate the object by passing the arguments for the class variables
archiveObj = ArchiveFiles("dbacademy_silver", "events_silver", "event_time", "755", "/tmp/hari/delta")

# COMMAND ----------

# DBTITLE 1,Call the archiveDelta method
archiveObj.archiveDelta()

# COMMAND ----------

# DBTITLE 1,Call the dropOldData method
archiveObj.dropOldData()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to manually verify if the data has been written to archive path
# MAGIC DESCRIBE HISTORY delta.`/tmp/hari/delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to manually verify if dtaa has been deleted from the target table that is older than the specified threshold days
# MAGIC select min(event_time), max(event_time) from dbacademy_silver.events_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY dbacademy_silver.events_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to restore the table manually - in order to just perform the above operation again - only for the purpose of testing
# MAGIC RESTORE TABLE dbacademy_silver.events_silver VERSION AS OF 8

# COMMAND ----------

# DBTITLE 1,Next steps
# Additional condition to be added to the filter in order to filter only the from the last run date of the archival process
# Since the old data from the target delta is being archived, the existing process is sufficient to ensure that duplicate data is not appended to the archive path
# Still a last_run_date along with timestamp could be useful to have better control over identifying the old data in a delta table
last_run_date = spark.sql("select max(event_time) from delta.`/tmp/hari/delta`").collect()[0][0{;.}]

print(last_run_date)

# COMMAND ----------

# DBTITLE 1,Next steps
# Add logger to the code to display custom log on console or to write to custom log files

import logging
logger = logging.getLogger("demo logger")
logger.setLevel(logging.WARN)


# COMMAND ----------

logger.warning("this is a warn message")
