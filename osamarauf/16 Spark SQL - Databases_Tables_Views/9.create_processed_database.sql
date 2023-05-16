-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/covid19projectdatalake/processed"

-- COMMAND ----------

DESC EXTENDED f1_processed
