# Databricks notebook source
dbutils.widgets.text('input', '', 'Enter the para value')

# COMMAND ----------

input_param = dbutils.widgets.get('input')

# COMMAND ----------

print(input_param)

# COMMAND ----------

dbutils.notebook.exit(100)

# COMMAND ----------


