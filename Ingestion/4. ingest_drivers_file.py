# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Ingest Drivers file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#defining name schema first
#as name column has nested json object

name_schema = StructType(fields= [StructField("forename", StringType(), True),
                                  StructField("surname", StringType(), True)

])

# COMMAND ----------

#defining df schema

drivers_schema = StructType(fields= [ StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", name_schema),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)

])


# COMMAND ----------

#reading the file
drivers_df = spark.read.schema(drivers_schema).json('/mnt/udemystrg/raw/raw/drivers.json')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Rename columns and add new columns

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                .withColumnRenamed('driverRef', 'driver_ref') \
                                .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Drop Unwanted Columns

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop('url')
#display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Write the output to processed folder
# MAGIC

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet('/mnt/udemystrg/processed/drivers')
