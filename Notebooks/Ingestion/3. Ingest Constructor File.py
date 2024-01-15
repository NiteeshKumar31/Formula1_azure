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
# MAGIC #### Step1: Read constructors file form bronze container

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef String, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{bronze_folder_path}/{v_file_date}/constructors.json")
# constructors_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step2: Drop unwanted columns

# COMMAND ----------

constructors_drop_columns_df = constructors_df.drop("url")
# constructors_drop_columns_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Step2: Select and Rename required columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructors_final_df = constructors_drop_columns_df.withColumnRenamed("constructorId", "constructor_id")\
                                                    .withColumnRenamed("ConstructorRef", "constructor_ref")\
                                                    .withColumn("ingestion_date", current_timestamp())\
                                                    .withColumn("data_source", lit(v_data_source))\
                                                    .withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

# constructors_final_df.write.parquet(f"{silver_folder_path}/constructors")
constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_silver.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_silver.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")