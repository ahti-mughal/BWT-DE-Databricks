# Databricks notebook source
print("I am child notebook")

# COMMAND ----------

dbutils.notebook.exit(100)

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("input","","Enter the input")

# COMMAND ----------

input_param = dbutils.widgets.get("input")

# COMMAND ----------

input_param

# COMMAND ----------

