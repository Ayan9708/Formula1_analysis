# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest pit_stops.json_file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read the file using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------


#defining schema for the df

pit_stops_schema = StructType(fields = [
                                StructField('raceId', IntegerType(), False),
                                StructField('driverId', IntegerType(), False),
                                StructField('stop', IntegerType(), False),
                                StructField('lap', IntegerType(), False),
                                StructField('time', StringType(), False),
                                StructField('duration', StringType(), True),
                                StructField('milliseconds', StringType(), True)

])

# COMMAND ----------

#reading multiline json file
pit_stops_df = spark.read.schema(pit_stops_schema).option('multiline', True).json('/mnt/udemystrg/raw/raw/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Rename columns and add new columns

# COMMAND ----------

pit_stops_processed_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Write the output to processed file

# COMMAND ----------

pit_stops_processed_df.write.mode('overwrite').parquet('/mnt/udemystrg/processed/pit_stops')
