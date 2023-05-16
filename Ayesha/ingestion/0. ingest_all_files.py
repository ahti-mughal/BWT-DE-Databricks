# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

#v_run=dbutils.notebook.run("1.Ingest_Circuit_File",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_run

# COMMAND ----------

v_run=dbutils.notebook.run("2.Ingest_race_files",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

v_run=dbutils.notebook.run("3.ingest_constructor_file",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

v_run=dbutils.notebook.run("4.ingest_drivers_file",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

v_run=dbutils.notebook.run("5.ingest_result_file",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

v_run=dbutils.notebook.run("6. ingest_pit_stops_file",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

v_run=dbutils.notebook.run("7. ingest_lap_times_file",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

v_run=dbutils.notebook.run("8. ingest_qualifying_file",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

v_run

# COMMAND ----------

