-- Databricks notebook source
create or replace temp view v_dominant_team
as
SELECT team_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points, rank() OVER(order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

select * from v_dominant_team

-- COMMAND ----------

SELECT race_year, team_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_team where team_rank <= 10)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


