# Databricks notebook source
# MAGIC %md
# MAGIC ### Analysing Driver's Standing in the Formula1 race
# MAGIC ##### 1. Reading the results data
# MAGIC ##### 2. Grouping the data by year, team
# MAGIC

# COMMAND ----------

# MAGIC %run "../set_up/config"

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, count, rank, when, col, desc

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Reading the results data

# COMMAND ----------

#reading the race_results data
race_results_df = spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Grouping the data by year, team

# COMMAND ----------

constructor_standings_df = race_results_df \
                            .groupBy('race_year', 'team') \
                            .agg(sum('points').alias('total_points'),
                                 count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Giving ranks to the data

# COMMAND ----------

constructor_standings_spec = Window.partitionBy('race_year').orderBy(desc(col('total_points')), desc(col('wins')))
final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_standings_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing the data into constructor_standings file

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_path}/constructor_standings')
