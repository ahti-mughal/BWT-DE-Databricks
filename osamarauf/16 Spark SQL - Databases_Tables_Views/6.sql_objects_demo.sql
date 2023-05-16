-- Databricks notebook source
SHOW TABLES;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

Use demo

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration""

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

select * from demo.race_results_python

-- COMMAND ----------

CREATE TABLE demo.race_results_sql 
AS
SELECT * from demo.race_results_python;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
( 
  race_year INT,
  race_name STRING, 
  race_date TIMESTAMP, 
  circuit_location STRING, 
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING, 
  grid INT,
  fastest_lap INT, 
  race_time STRING,
  points FLOAT, 
  position INT 
)
USING parquet 
LOCATION "/mnt/covid19projectdatalake/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN DEMO;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

select * from demo.race_results_ext_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------


