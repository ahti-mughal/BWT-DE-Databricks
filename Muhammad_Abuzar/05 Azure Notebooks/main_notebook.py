# Databricks notebook source
print('Hello')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 'Hello'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for folder in dbutils.fs.ls('/'):
    print(folder)

# COMMAND ----------

dbutils.fs.help('mount')

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run('./child_notebook', 10, {'input': 'Called from Main'})

# COMMAND ----------


