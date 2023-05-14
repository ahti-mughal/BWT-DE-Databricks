-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT *, concat(driver_ref, '-', code) AS new_driver_ref
FROM driver

-- COMMAND ----------

SELECT *, split(name, ' ')[0] forename, split(name, ' ')[1] surename 
FROM driver

-- COMMAND ----------

select nationality, count(*) from driver group by nationality order by nationality

-- COMMAND ----------

select nationality, count(*) from driver group by nationality having count(*) > 100 order by nationality

-- COMMAND ----------

SELECT nationality, name, dob, rank() OVER (Partition by nationality ORDER BY dob DESC) as age_rank
from driver
ORDER BY nationality, age_rank

-- COMMAND ----------


