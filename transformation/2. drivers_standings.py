# Databricks notebook source
# MAGIC %md
# MAGIC ### Analysing Driver's Standing in the Formula1 race
# MAGIC ##### 1. Reading the results data
# MAGIC ##### 2. Grouping the data by year, driver, team, nationality
# MAGIC ##### 3. Giving ranks to the data
# MAGIC

# COMMAND ----------

# MAGIC %run "../set_up/config"

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, when, col, count, desc, rank

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Reading the results data

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Grouping the data by year, driver, team, nationality

# COMMAND ----------

driver_standings_df = race_results_df \
                        .groupBy('race_year', 'drivers_name', 'team', 'drivers_nationality') \
                        .agg(sum('points').alias('total_points'), 
                             count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Giving ranks to the data

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing the data in driver_standings file

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_path}/driver_standings')
