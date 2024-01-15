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
# MAGIC #### Step1: Read the drivers nested json file by creating schema

# COMMAND ----------

drivers_df = spark.read.json(f"{bronze_folder_path}/{v_file_date}/drivers.json")
# drivers_df.show()

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema= StructType (fields = [StructField("forename", StringType(), False),
                                   StructField("surname", StringType(), False)]
                        )

drivers_schema = StructType( fields = [StructField("code", StringType(), True ),
                                       StructField("dob",  DateType(), True ),
                                       StructField("driverId", IntegerType(), True ),
                                       StructField("driverRef", StringType(), True ),
                                       StructField("name", name_schema),
                                       StructField("nationality", StringType(), True ),
                                       StructField("number", StringType(), True ),
                                       StructField("url", StringType(), True ),
                                      ]
    
)

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{bronze_folder_path}/{v_file_date}/drivers.json")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2: Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit,concat

# COMMAND ----------

drivers_rename_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("driverRef", "driver_ref")\
                                .withColumn("ingestion_date", current_timestamp())\
                                .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step3: Drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_rename_df.drop(col('url'))

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_silver.drivers")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from f1_silver.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")