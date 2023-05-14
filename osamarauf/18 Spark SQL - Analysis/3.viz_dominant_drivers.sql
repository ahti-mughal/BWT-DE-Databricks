-- Databricks notebook source
create or replace temp view v_dominant_drivers
as
SELECT driver_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points, rank() OVER(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, driver_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <=10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


