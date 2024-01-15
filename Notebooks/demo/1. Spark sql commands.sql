-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE DEMO

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

