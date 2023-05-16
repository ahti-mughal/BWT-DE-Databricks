# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuit.csv Files

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1: Read the csv File using The Saprk DataFrameReader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema= StructType(fields=[StructField("circuitId",IntegerType(),False),
StructField("circuitRef",StringType(),True),
StructField("name",StringType(),True),
StructField("location",StringType(),True),
StructField("country",StringType(),True),
StructField("lat",DoubleType(),True),
StructField("lng",DoubleType(),True),
StructField("alt",IntegerType(),True),
StructField("url",StringType(),True),
])          

# COMMAND ----------

#Bad PERFORMANCE
#circuits_dataframe = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/correctformula1/raw/#circuits.csv")
#IMPROVED PERFORMANCE
circuits_dataframe = spark.read.option("header",True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/raw

# COMMAND ----------

type(circuits_dataframe)

# COMMAND ----------

circuits_dataframe.show()

# COMMAND ----------

display(circuits_dataframe)

# COMMAND ----------

circuits_dataframe.printSchema()

# COMMAND ----------

circuits_dataframe.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Select Only the Required Columns

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df=circuits_dataframe.select(F.col("circuitId"),F.col("circuitRef"),F.col("name"),\
F.col("location"),F.col("country"),F.col("lat"),F.col("lng"),F.col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Rename Columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
            .withColumnRenamed("circuitRef","circuit_ref")\
            .withColumnRenamed("lng","longitude")\
            .withColumnRenamed("alt","altitude")\
            .withColumnRenamed("lat","latitude")\
            .withColumn("data_source",lit(v_data_source))\
            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step4: Add Ingestion Data To Dataframe

# COMMAND ----------

# If we want to add the literal as the column then it will give error then 
circuits_final = circuits_renamed_df.withColumn("env",F.lit("production"))

# COMMAND ----------

#It will adding the datetime as column object 
#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",F.current_timestamp())#previous code
circuits_final_df =add_ingestion_date(circuits_renamed_df)#improved code

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step5: Write Data to DataLake As Parquet

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.circuits

# COMMAND ----------

#overwrite_partition(circuits_final_df,"f1_processed","circuits","circuit_id")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls {processed_folder_path}/circuits

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

