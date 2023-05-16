# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Pit_Stops.json File

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1:Read the json File Using the Spark Dataframe Reader

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

pit_stops_schema= StructType(fields=[StructField("raceId",IntegerType(),False),
StructField("driverId",IntegerType(),True),
StructField("stop",StringType(),True),
StructField("lap",IntegerType(),True),
StructField("time",StringType(),True),
StructField("duration",StringType(),True),
StructField("milliseconds",IntegerType(),True),
])   

# COMMAND ----------

pit_stops_df = spark.read.option("header",True).option("multiline",True).schema(pit_stops_schema).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Reaname Columns & Add New Columns

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

pit_stops_final_df=pit_stops_df.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('raceId','race_id')\
.withColumn("ingestion_date",F.current_timestamp())\
 .withColumn("file_date",F.lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Write Output to Parquet File

# COMMAND ----------

#pit_stops_final_df.write.mode('overwrite').parquet('/mnt/correctformula1/processed/pit_stops')

#pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.pit_stops

# COMMAND ----------

#overwrite_partition(pit_stops_final_df,"f1_processed","pit_stops","race_id")
merge_condition ="tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_final_df,'f1_processed','pit_stops',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/processed/pit_stops

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

