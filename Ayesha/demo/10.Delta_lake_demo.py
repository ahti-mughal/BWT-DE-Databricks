# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/correctformula1/demo'

# COMMAND ----------

results_df =spark.read.option("inferSchema",True)\
        .json('/mnt/correctformula1/raw/2021-03-28/results.json')

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save('/mnt/correctformula1/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta location '/mnt/correctformula1/demo/results_external'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_ext_df =spark.read.format("delta").load('/mnt/correctformula1/demo/results_external')

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update & Delete From Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points = 11 - position
# MAGIC where position <=10 

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/correctformula1/demo/results_managed")
deltaTable.update("position <=10 ",{"points" : "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete f1_demo.results_managed
# MAGIC where position > 10 

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/correctformula1/demo/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using Merge 

# COMMAND ----------

drivers_day1_df =  spark.read.option("inferSchema",True)\
        .json('/mnt/correctformula1/raw/2021-03-28/drivers.json') \
         .filter("driverId <= 10 ")\
                .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df =  spark.read.option("inferSchema",True)\
        .json('/mnt/correctformula1/raw/2021-03-28/drivers.json')\
            .filter("driverId BETWEEN 6 AND 15")\
                .select("driverId","dob",upper("name.forename").alias("forename"),"name.surname")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df =  spark.read.option("inferSchema",True)\
        .json('/mnt/correctformula1/raw/2021-03-28/drivers.json')\
            .filter("driverId BETWEEN 1 AND 5 or driverId BETWEEN 16 AND 20")\
                .select("driverId","dob",upper("name.forename").alias("forename"),"name.surname")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.driver_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA location "/mnt/correctformula1/demo/driver_merge"

# COMMAND ----------

# MAGIC %sql 
# MAGIC --DROP TABLE f1_demo.driver_merge

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.driver_merge tgt
# MAGIC USING drivers_day1  upd 
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC update set 
# MAGIC tgt.dob = upd.dob,
# MAGIC tgt.forename = upd.forename,
# MAGIC tgt.surname = upd.surname,
# MAGIC tgt.updatedDate =current_timestamp
# MAGIC when not matched 
# MAGIC then insert (driverId,dob,forename,surname,createdDate) values(driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.driver_merge tgt
# MAGIC USING drivers_day2 upd 
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC update set 
# MAGIC tgt.dob = upd.dob,
# MAGIC tgt.forename = upd.forename,
# MAGIC tgt.surname = upd.surname,
# MAGIC tgt.updatedDate =current_timestamp
# MAGIC when not matched 
# MAGIC then insert (driverId,dob,forename,surname,createdDate) values(driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------


deltaTable = DeltaTable.forPath(spark, "/mnt/correctformula1/demo/driver_merge")

deltaTable.alias("tgt").merge(drivers_day3_df.alias("upd"),"tgt.driverId = upd.driverId")\
    .whenMatchedUpdate(set = {"dob": "upd.dob","forename": "upd.forename","surname": "upd.surname","updatedDate": "current_timestamp()"})\
       .whenNotMatchedInsert(values = {"upd.driverId" : "upd.driverId",
        "dob": "upd.dob","forename": "upd.forename","surname": "upd.surname","updatedDate": "current_timestamp()"})\
            .execute() 

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo.driver_merge

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo.driver_merge version as of 2 

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo.driver_merge TIMESTAMP AS OF '2023-05-14T18:00:45.000+0000'

# COMMAND ----------

