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

qualifying_schema= StructType(fields=[StructField("qualifyId",IntegerType(),False),
StructField("raceId",IntegerType(),True),
StructField("driverId",IntegerType(),True),
StructField("constructorId",IntegerType(),True),
StructField("number",IntegerType(),True),
StructField("position",IntegerType(),True),
StructField("q1",StringType(),True),
StructField("q2",StringType(),True),
StructField("q3",StringType(),True),
])   

# COMMAND ----------

qualifying_df = spark.read.option("header",True).option("multiline",True).schema(qualifying_schema).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Reaname Columns & Add New Columns

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

final_df=qualifying_df.withColumnRenamed('qualifyId','qualify_id')\
.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('raceId','race_id')\
.withColumnRenamed('constructorId','constructor_id')\
.withColumn("ingestion_date",F.current_timestamp())\
    .withColumn("file_date",F.lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Write Output to Parquet File

# COMMAND ----------

#final_df.write.mode('overwrite').parquet('/mnt/correctformula1/processed/qualifying')
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.qualifying

# COMMAND ----------

#overwrite_partition(final_df,"f1_processed","qualifying","race_id")
merge_condition ="tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df,'f1_processed','qualifying',processed_folder_path,merge_condition,'race_id')


# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/processed/qualifying

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

