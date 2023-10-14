# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting qualifying split csv files from a folder

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Read the csv files by dataframe reader api

# COMMAND ----------

# MAGIC %run "../set_up/config"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------


#defining schema for the df

qualifying_schema = StructType(fields = [
                                StructField('qualifyId', IntegerType(), False),
                                StructField('raceId', IntegerType(), True),
                                StructField('driverId', IntegerType(), True),
                                StructField('constructorId', IntegerType(), True),
                                StructField('number', IntegerType(), True),
                                StructField('position', IntegerType(), True),
                                StructField('q1', StringType(), True),
                                StructField('q2', IntegerType(), True),
                                StructField('q3', IntegerType(), True)


])

# COMMAND ----------

#reading the files
#reads multiple lines by * wildcard
qualifying_df = spark.read.schema(qualifying_schema).option('multiline', True).json(f'{raw_path}/qualifying/qualifying_split*.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Rename and add columns

# COMMAND ----------

qualifying_processed_df = qualifying_df.withColumnRenamed('raceId', 'race_id') \
                                    .withColumnRenamed('qualifyId', 'qualify_id') \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('constructorId', 'constructor_id') \
                                    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Write the output to processed folder

# COMMAND ----------

qualifying_processed_df.write.mode('overwrite').parquet(f'{processed_path}/qualifying')

# COMMAND ----------


