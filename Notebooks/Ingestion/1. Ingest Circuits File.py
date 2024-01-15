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
# MAGIC #### Step1: Read circuits file from bronze container

# COMMAND ----------

# display(dbutils.fs.mounts())
%ls

display(dbutils.fs.ls(f"{bronze_folder_path}"))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step2: Create external schema and read the circuits file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

 circuit_schema = StructType (fields = [StructField("circuitId", IntegerType(), False),
                                StructField("circuitRef", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("location", StringType(), True),
                                StructField("country", StringType(), True),
                                StructField("lat", DoubleType(), True),
                                StructField("lng", DoubleType(), True),
                                StructField("alt", IntegerType(), True),
                                StructField("url", StringType(), True)
                                ]
                      )

# COMMAND ----------

circuits_df = spark.read.option("header", True).option("schema", circuit_schema).csv(f"{bronze_folder_path}/{v_file_date}/circuits.csv")

# display(circuits_df)

# COMMAND ----------

# circuits_df.printSchema()

# COMMAND ----------

# circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4: Rename columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_renamed_df = circuits_df.withColumnRenamed("circuitId", "circuit_id")\
                                .withColumnRenamed("circuitRef", "circuit_ref")\
                                .withColumnRenamed("lat", "latitude")\
                                .withColumnRenamed("lng", "longitude")\
                                .withColumnRenamed("alt", "altitude")\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))   
                                

# COMMAND ----------

# circuit_renamed_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step5: Add current timestamp to data frame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuit_renamed_df)

display(circuits_final_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Step6: Write data into datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_silver.circuits")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from f1_silver.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")