# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Lap_Times Folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1:Read the CSV File Using the Spark Dataframe Reader API

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

lap_times_schema= StructType(fields=[StructField("raceId",IntegerType(),False),
StructField("driverId",IntegerType(),True),
StructField("lap",IntegerType(),True),
StructField("position",IntegerType(),True),
StructField("time",StringType(),True), 
StructField("milliseconds",IntegerType(),True),
])   

# COMMAND ----------

lap_times_df = spark.read.option("header",True).schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Reaname Columns & Add New Columns

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

lap_times_final_df=lap_times_df.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('raceId','race_id')\
.withColumn("ingestion_date",F.current_timestamp())\
.withColumn("file_date",F.lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Write Output to Parquet File

# COMMAND ----------

#lap_times_final_df.write.mode('overwrite').parquet('/mnt/correctformula1/processed/lap_times')

#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.lap_times

# COMMAND ----------

#overwrite_partition(lap_times_final_df,"f1_processed","lap_times","race_id")
merge_condition ="tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df,'f1_processed','lap_times',processed_folder_path,merge_condition,'race_id')


# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/processed/lap_times

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

