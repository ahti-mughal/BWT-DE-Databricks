# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest Result.json file 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the CSV file using Spark Dataframe Reader API

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-04-18")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.read.json("/mnt/correctformula1/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  raceId ,count(1)
# MAGIC FROM results_w2
# MAGIC GROUP BY raceId

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DoubleType,DateType,FloatType

# COMMAND ----------

result_schema= StructType(fields=[StructField("resultId",IntegerType(),False),
StructField("raceId",IntegerType(),True),
StructField("driverId",IntegerType(),True),
StructField("constructorId",IntegerType(),True),
StructField("number",IntegerType(),True),
StructField("grid",IntegerType(),True),
StructField("position",IntegerType(),True),
StructField("positionText",StringType(),True),
StructField("positionOrder",IntegerType(),True),
StructField("points",FloatType(),True),
StructField("laps",IntegerType(),True),
StructField("time",StringType(),True),
StructField("fastestLap",IntegerType(),True),
StructField("rank",IntegerType(),True),
StructField("fastestLapTime",StringType(),True),
StructField("fastestLapSpeed",FloatType(),True),
StructField("statusId", StringType(),True),
]) 

# COMMAND ----------

results_df=spark.read.option("header",True).schema(result_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Select the columns Required and Rename them

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_wih_column_df=results_df.withColumnRenamed('resultId','result_id')\
                    .withColumnRenamed('raceId','race_id')\
                    .withColumnRenamed('driverId','driver_id')\
                    .withColumnRenamed('constructorId','constructor_id')\
                    .withColumnRenamed('positionText','position_text')\
                    .withColumnRenamed('positionOrdrer','position_ordrer')\
                    .withColumnRenamed('fastestLap','fastest_lap')\
                    .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                    .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                    .withColumn("ingestion_date",F.current_timestamp())\
                    .withColumn("file_date",F.lit(v_file_date))
   

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Drop Unwanted Column

# COMMAND ----------

results_final_df=results_wih_column_df.drop(F.col('statusId'))

# COMMAND ----------
# MAGIC %md
# MAGIC ##### De-dupe the Dataframe
results_dedupped_df =results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write the Output to Processed Container in Parquet Format

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### first Method

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id ={race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/correctformula1/processed/results')

#results_final_df.write.mode("append").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Second Method

# COMMAND ----------

#overwrite_partition(results_final_df,"f1_processed","results","race_id")
merge_condition ="tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_dedupped_df,'f1_processed','results',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.results

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/correctformula1/processed/results

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id ,count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id

# COMMAND ----------

