# Databricks notebook source
# MAGIC %run  "../include/common_functions"

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_df =spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location","circuit_location")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

constructor_df =spark.read.format("delta").load(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name","team")

# COMMAND ----------

driver_df =spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

races_df =spark.read.format("delta").load(f"{processed_folder_path}/races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df =spark.read.format("delta").load(f"{processed_folder_path}/results")\
    .filter(f"file_date = '{v_file_date}'")\
    .withColumnRenamed("file_date","results_file_date")\
    .withColumnRenamed("time","race_time")\
    .withColumnRenamed("race_id","results_race_id")     

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df,circuits_df.circuit_id == races_df.circuit_id,"inner")\
.select(races_df.race_id,races_df.race_year,
races_df.race_name,circuits_df.circuit_location,races_df.race_date)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Join Results to all other Dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df,results_df.results_race_id == race_circuits_df.race_id)\
.join(driver_df,results_df.driver_id == driver_df.driver_id)\
 .join(constructor_df,results_df.constructor_id == constructor_df.constructor_id)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","results_file_date")\
    .withColumn("created_date",current_timestamp())\
    .withColumnRenamed("file_date","results_file_date")
    

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
#overwrite_partition(final_df,"f1_presentation","race_results","race_id")
merge_condition ="tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df,'f1_presentation','race_results',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC --drop table f1_presentation.race_results

# COMMAND ----------

display(final_df.results_file_date)

# COMMAND ----------

