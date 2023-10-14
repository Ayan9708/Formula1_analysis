# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Read the data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#defing the df schema

results_schema = StructType(fields= [
                                        StructField('resultId', IntegerType(), False),
                                        StructField('raceId', IntegerType(), False),
                                        StructField('driverId', IntegerType(), False),
                                        StructField('constructorId', IntegerType(), False),
                                        StructField('number', IntegerType(), True),
                                        StructField('grid', IntegerType(), False),
                                        StructField('position', IntegerType(), True),
                                        StructField('positionText', StringType(), False),
                                        StructField('positionOrder', IntegerType(), False),
                                        StructField('points', DoubleType(), False),
                                        StructField('laps', IntegerType(), False),
                                        StructField('time',StringType(), True),
                                        StructField('milliseconds', IntegerType(), True),
                                        StructField('fastestLap', IntegerType(), True),
                                        StructField('rank', IntegerType(), True),
                                        StructField('fastestLapTime', StringType(), True),
                                        StructField('fastestLapSpeed', StringType(), True),
                                        StructField('statusId', IntegerType(), False)

])

# COMMAND ----------

#reading the data

results_df = spark.read.schema(results_schema).json('/mnt/udemystrg/raw/raw/results.json')

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Adding, Removing and Renaming columns

# COMMAND ----------

results_processed_df = results_df.withColumnRenamed('resultId', 'result_id') \
                                .withColumnRenamed('raceId', 'race_id') \
                                .withColumnRenamed('driverId', 'driver_id') \
                                .withColumnRenamed('constructorId', 'constructor_id') \
                                .withColumnRenamed('positionText', 'position_text') \
                                .withColumnRenamed('positionOrder', 'position_order') \
                                .withColumnRenamed('fastestLap', 'fastest_lap') \
                                .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
                                .withColumn('ingestion_date', current_timestamp()) \
                                .drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Writing the df into parquet with partion by race_id

# COMMAND ----------

results_processed_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/udemystrg/processed/results')

# COMMAND ----------


