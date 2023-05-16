# Databricks notebook source
# MAGIC %md
# MAGIC #NoteBook Introduction

# COMMAND ----------

name= "amna"

# COMMAND ----------

print(name)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT "amna"

# COMMAND ----------

# MAGIC %scala
# MAGIC val msg="Hello"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/COVID/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/COVID/USAFacts/

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/databricks-datasets/COVID/USAFacts/covid_confirmed_usafacts.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# MAGIC %md
# MAGIC #DataBricks Utilities

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for folderName in dbutils.fs.ls('/'):
    print(folderName)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("mount")

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run('./child_notebook',10)

# COMMAND ----------

dbutils.notebook.run('./child_notebook',10,{"input":"Called from main Notebook"})

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install pandas