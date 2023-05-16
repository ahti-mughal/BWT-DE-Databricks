-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Managed Tables

-- COMMAND ----------

-- MAGIC %run  "../include/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE  race_year =2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT * FROM demo.race_results_python WHERE  race_year =2020;



-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

Show tables in demo;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### External Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql (
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT ,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT ,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/correctformula1/presentation/race_results_ext_sql"


-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;


-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql ;


-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;


-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Views on Tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_reults
AS 
SELECT * FROM demo.race_results_python 
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_reults;


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_reults
AS 
SELECT * FROM demo.race_results_python 
WHERE race_year = 2012;


-- COMMAND ----------

SELECT * FROM global_temp.gv_race_reults;


-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_reults
AS 
SELECT * FROM demo.race_results_python 
WHERE race_year = 2012;


-- COMMAND ----------

SELECT * FROM demo.pv_race_reults

-- COMMAND ----------

