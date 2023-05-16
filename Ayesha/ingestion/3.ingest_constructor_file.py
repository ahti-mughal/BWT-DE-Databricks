# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Constructor.json File 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1:Read the json File Using the Spark Dataframe Reader

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

constructor_schema="constructorId INT ,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df= spark.read\
    .schema(constructor_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Drop the unwanted Columns From DataFrame 

# COMMAND ----------

constructor_drpped_df= constructor_df.drop('url')

# COMMAND ----------

display(constructor_drpped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Reaname Columns & Add Ingestion Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_renamed_df = constructor_drpped_df.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef","constructor_ref")\
    .withColumn("file_date",lit(v_file_date)
             )

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_renamed_df)
#.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4: Write Output to Parquet File

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.constructors

# COMMAND ----------

#overwrite_partition(constructor_final_df,"f1_processed","constructors","constructor_id")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/processed/constructors

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

