# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1:  Read the CSV file using Spark Dataframe Reader API

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

races_schema= StructType(fields=[StructField("raceId",IntegerType(),False),
StructField("year",IntegerType(),True),
StructField("round",IntegerType(),True),
StructField("circuitId",IntegerType(),True),
StructField("name",StringType(),True),
StructField("date",DateType(),True),
StructField("time",StringType(),True),
StructField("url",StringType(),True),
])   

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Add Ingestion Data To Dataframe

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql.functions import col,lit ,to_timestamp,concat

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df)\
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------


#races_df.withColumn("ingestion_date",F.current_timestamp())\
display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step3: Select the columns Required and Rename them

# COMMAND ----------

 from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp_df.withColumnRenamed("raceId","race_id")\
        .withColumnRenamed("year","race_year")\
        .withColumnRenamed("circuitId","circuit_id")\
        .withColumn("file_date",lit(v_file_date)
             )

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write the Output to Processed Container in Parquet Format

# COMMAND ----------



# COMMAND ----------

#races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

races_selected_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

#overwrite_partition(races_selected_df,"f1_processed","races","race_id")


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables in  f1_processed

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/processed/races

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

