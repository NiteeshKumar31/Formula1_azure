# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step1:Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{bronze_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2: Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

# for race_id_list in final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_silver.pit_stops")):
#         spark.sql(f"ALTER TABLE f1_silver.pit_stops DROP IF EXISTS PARTITION (race_id={race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3: Write to output to processed container in parquet format

# COMMAND ----------

# # final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/pit_stops")
# final_df.write.mode("append").format("parquet").saveAsTable("f1_silver.pit_stops")

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_silver', 'pit_stops', silver_folder_path, merge_condition, 'race_id')


# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, count(1)
# MAGIC from f1_silver.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- drop table f1_silver.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_silver.pit_stops

# COMMAND ----------

