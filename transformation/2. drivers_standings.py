# Databricks notebook source
# MAGIC %run "../set_up/config"

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, when, col, count, desc, rank

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

driver_standings_df = race_results_df \
                        .groupBy('race_year', 'drivers_name', 'team', 'drivers_nationality') \
                        .agg(sum('points').alias('total_points'), 
                             count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_path}/driver_standings')
