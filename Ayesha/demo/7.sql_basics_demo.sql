-- Databricks notebook source
SELECT * FROM f1_processed.drivers LIMIT 10;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers
WHERE nationality = 'British'
and dob >= '1990-01-01'

-- COMMAND ----------

SELECT * FROM f1_processed.drivers
WHERE nationality = 'British'
and dob >= '1990-01-01'
ORDER BY dob DESC ;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers
ORDER BY nationality ASC,
dob DESC ;

-- COMMAND ----------

SELECT nationality,name,dob FROM f1_processed.drivers
WHERE (nationality = 'British'
and dob >= '1990-01-01') OR nationality ='Indian'
ORDER BY dob DESC ;

-- COMMAND ----------

