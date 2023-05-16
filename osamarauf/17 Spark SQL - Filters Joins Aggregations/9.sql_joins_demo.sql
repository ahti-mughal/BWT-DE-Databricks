-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standing_2018
as 
SELECT race_year, driver_name, team, total_points, wins, rank from driver_standings
where race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standing_2020
as 
SELECT race_year, driver_name, team, total_points, wins, rank from driver_standings
where race_year = 2020;

-- COMMAND ----------

select *
from v_driver_standing_2018 d_2018
join v_driver_standing_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

select *
from v_driver_standing_2018 d_2018
left join v_driver_standing_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

select *
from v_driver_standing_2018 d_2018
right join v_driver_standing_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------


