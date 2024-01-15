# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step1: Read Races file from bronze container

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType (fields = [StructField("raceId", IntegerType(), False),
                                StructField("year", IntegerType(), True),
                                StructField("round", IntegerType(), True),
                                StructField("circuitId", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("date", DateType(), True),
                                StructField("time", StringType(), True),
                                StructField("url", StringType(), True)
                                ]
                      )

# COMMAND ----------

races_df = spark.read.option("header", True).option("schema", races_schema).csv(f"{bronze_folder_path}/{v_file_date}/races.csv")
# display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step2: Add Ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp())\
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                  .withColumn("data_source", lit(v_data_source))\
                                  .withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Step3: Select and Rename columns

# COMMAND ----------

races_select_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"),col("name"), col("ingestion_date"), col("race_timestamp"), col("data_source"))

# COMMAND ----------

# races_select_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{silver_folder_path}/races")
races_select_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_silver.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_silver.races

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

