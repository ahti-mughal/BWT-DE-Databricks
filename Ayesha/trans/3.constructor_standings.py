# Databricks notebook source
# MAGIC %md 
# MAGIC #### Produce Constructor Standings

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

# MAGIC %run  "../include/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df =spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"results_file_date = '{v_file_date}'")

# COMMAND ----------



# COMMAND ----------

race_results_list=df_column_to_list(race_results_df,'race_year')

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_results_list))

# COMMAND ----------


from pyspark.sql.functions import col,count,when,sum

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year","team")\
.agg(sum("points").alias("total_points"),count(when(col("position")==1, True)).alias("wins"))


# COMMAND ----------

display(constructor_standings_df.filter("race_year =2020 "))

# COMMAND ----------

from pyspark.sql.window  import Window
from pyspark.sql.functions import desc,asc,rank

# COMMAND ----------

driver_Rank_Spec= Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df =constructor_standings_df.withColumn("rank",rank().over(driver_Rank_Spec))

# COMMAND ----------

display(final_df.filter("race_year =2020 "))

# COMMAND ----------

#final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/constructor_standings")

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
merge_condition ="tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df,'f1_presentation','constructor_standings',processed_folder_path,merge_condition,'race_year')

# COMMAND ----------

overwrite_partition(final_df,"f1_presentation","constructor_standings","race_year")
