# Databricks notebook source
# MAGIC %md 
# MAGIC #### Spark Join Transformation

# COMMAND ----------

# MAGIC %run  "../include/configuration"

# COMMAND ----------

circuits_df =spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id <= 70")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df =spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019 ")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Outer Join

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"left")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# Right Outer Join
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"right")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# Full Join
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"full")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"semi")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Joins

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"anti")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross Joins

# COMMAND ----------

race_circuits_df =circuits_df.crossJoin(races_df)

# COMMAND ----------

