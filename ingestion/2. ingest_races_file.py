# Databricks notebook source
# MAGIC %run "../set_up/config"

# COMMAND ----------

# MAGIC %run "../set_up/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC #####Reading Races file with defined schema

# COMMAND ----------

races_schema = StructType(fields= [ StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True ),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", StringType(), True),
                                    StructField("time", StringType(), True)

]
)

# COMMAND ----------

races_df = spark.read.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_path}/races.csv")
races_df_select = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))


# COMMAND ----------

# MAGIC %md
# MAGIC #####Adding race_timestamp with date and time
# MAGIC #####Adding ingestion_date
# MAGIC #####Renaming column

# COMMAND ----------

races_df_renamed = races_df_select.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

races_with_timestamp_df = add_ingestion(races_df_renamed) \
                            .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Writing into parquet file

# COMMAND ----------

races_with_timestamp_df.write.mode("overwrite").partitionBy('race_year').parquet(f'{processed_path}/races')

# COMMAND ----------


