# Databricks notebook source
# MAGIC %run "../set_up/config"

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, count, rank, when, col, desc

# COMMAND ----------

#reading the race_results data
race_results_df = spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

constructor_standings_df = race_results_df \
                            .groupBy('race_year', 'team') \
                            .agg(sum('points').alias('total_points'),
                                 count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

constructor_standings_spec = Window.partitionBy('race_year').orderBy(desc(col('total_points')), desc(col('wins')))
final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_standings_spec))

# COMMAND ----------

display(final_df.filter('race_year = 2020'))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_path}/constructor_standings')

# COMMAND ----------


