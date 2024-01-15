-- Databricks notebook source
use f1_gold

-- COMMAND ----------

select * from f1_gold.calculated_race_results

-- COMMAND ----------

create or replace view dominant_teams_vw
as
select 
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) team_rank
from f1_gold.calculated_race_results
group by team_name
having count(1) >=100
order by avg_points desc

-- COMMAND ----------

select * from dominant_teams_vw

-- COMMAND ----------

select 
race_year,
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from calculated_race_results
where team_name in (select team_name from dominant_teams_vw where team_rank <=5)
group by race_year,team_name
order by race_year, avg_points desc

-- COMMAND ----------

