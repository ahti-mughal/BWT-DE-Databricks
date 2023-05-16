-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * from driver limit 10;

-- COMMAND ----------

DESC driver

-- COMMAND ----------

SELECT * from driver where nationality = "British" and dob >= "1990-01-01"

-- COMMAND ----------

SELECT name, dob from driver where nationality = "British" and dob >= "1990-01-01"

-- COMMAND ----------

SELECT * from driver ORDER BY nationality ASC, dob DESC

-- COMMAND ----------

SELECT name, dob from driver where (nationality = "British" and dob >= "1990-01-01") or nationality = "Germany" ORDER BY dob DESC

-- COMMAND ----------


