-- Databricks notebook source
use f1_gold

-- COMMAND ----------

create or replace temp view dominant_drivers_vw
as
select 
driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) driver_rank
from calculated_race_results
group by driver_name
having count(1) >=50
order by avg_points desc

-- COMMAND ----------

select * from dominant_drivers_vw

-- COMMAND ----------

select 
race_year,
driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from calculated_race_results
where driver_name in (select driver_name from dominant_drivers_vw where driver_rank <=10)
group by race_year,driver_name
order by race_year, avg_points desc

-- COMMAND ----------

