# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestive split csv files from a folder

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Read the csv files by dataframe reader api

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------


#defining schema for the df

lap_times_schema = StructType(fields = [
                                StructField('raceId', IntegerType(), False),
                                StructField('driverId', IntegerType(), True),
                                StructField('lap', IntegerType(), True),
                                StructField('position', IntegerType(), True),
                                StructField('time', StringType(), True),
                                StructField('milliseconds', IntegerType(), True)

])

# COMMAND ----------

#reading the files
#reads multiple lines by * wildcard
lap_times_df = spark.read.schema(lap_times_schema).csv('/mnt/udemystrg/raw/raw/lap_times/lap_times_split*.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Rename and add columns

# COMMAND ----------

lap_times_processed_df = lap_times_df.withColumnRenamed('raceId', 'race_id') \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Write the output to processed folder

# COMMAND ----------

lap_times_processed_df.write.mode('overwrite').parquet('/mnt/udemystrg/processed/lap_times')

# COMMAND ----------


