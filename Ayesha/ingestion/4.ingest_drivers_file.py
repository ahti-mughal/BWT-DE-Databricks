# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Drivers.json File 

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1:Read the json File Using the Spark Dataframe Reader

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

name_schema =StructType(fields=[
StructField("forename",StringType(),True),
StructField("surname",StringType(),True)])   

# COMMAND ----------

driver_schema= StructType(fields=[StructField("driverId",IntegerType(),False),
StructField("driverRef",StringType(),True),
StructField("number",IntegerType(),True),
StructField("code",StringType(),True),
StructField("name",name_schema),
StructField("dob",DateType(),True),
StructField("nationality",StringType(),True),
StructField("url",StringType(),True),
])   

# COMMAND ----------

driver_df = spark.read.option("header",True).schema(driver_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

display(driver_df)

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Reaname Columns & Add Ingestion Data

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_wih_column_df=driver_df.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('driverRef','driver_ref')\
.withColumn("ingestion_date",F.current_timestamp())\
    .withColumn("name",F.concat(F.col('name.forename') ,F.lit(' '),F.col('name.surname')) )\
   .withColumn("file_date",F.lit(v_file_date))
         

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3:Drop the unwanted Columns From DataFrame 

# COMMAND ----------

drivers_final_df=drivers_wih_column_df.drop(F.col('url'))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write Output to Parquet File

# COMMAND ----------

#drivers_final_df.write.mode('overwrite').parquet('/mnt/correctformula1/processed/drivers')

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.drivers

# COMMAND ----------

#overwrite_partition(drivers_final_df,"f1_processed","drivers","driver_id")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/processed/drivers

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

