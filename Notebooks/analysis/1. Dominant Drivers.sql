-- Databricks notebook source
use f1_gold

-- COMMAND ----------

select 
driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from calculated_race_results
where race_year between 2000 and 2010
group by driver_name
order by avg_points desc

-- COMMAND ----------

