-- Databricks notebook source
use f1_gold

-- COMMAND ----------

select * from f1_gold.calculated_race_results

-- COMMAND ----------

select 
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from calculated_race_results
where race_year between 2010 and 2020
group by team_name
order by avg_points desc

-- COMMAND ----------

