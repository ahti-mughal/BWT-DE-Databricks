-- Databricks notebook source
SELECT driver_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT driver_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY avg_points DESC
