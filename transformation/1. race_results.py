# Databricks notebook source
# MAGIC %md
# MAGIC ###Creating a Race_Results file before analysing standings

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Five files are used for analysing: races, circuits, drivers, constructors, results
# MAGIC ##### 2. Creating dataframe having columns similar to the BBC Sport api for standings
# MAGIC ##### 3. Write the dataframe into driver_standings file
# MAGIC ##### 4. Data can be analysed to give insights on driver's or team's performance by year or circuits

# COMMAND ----------

# MAGIC %run "../set_up/config"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading the relevant data from processed folder

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_path}/races') \
                            .withColumnRenamed('name', 'race_name') \
                            .withColumnRenamed('date', 'race_date') \
                            .withColumnRenamed('race_timestamp', 'race_date')


# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_path}/drivers') \
                        .withColumnRenamed('name', 'drivers_name') \
                        .withColumnRenamed('number', 'drivers_number') \
                        .withColumnRenamed('nationality', 'drivers_nationality')

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_path}/circuits') \
                        .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_path}/constructors') \
                        .withColumnRenamed('name', 'team')

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_path}/results') \
                        .withColumnRenamed('time', 'race_time')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Joining the files to obtain relevant information for analysing player or team standings

# COMMAND ----------

race_results_df = results_df.join(races_df, results_df.race_id == races_df.race_id, 'inner') \
                    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, 'inner') \
                    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, 'inner') \
                    .select('drivers_name','drivers_number', 'drivers_nationality', 'race_name', 'race_year', 'team', 'grid', 'fastest_lap', 'race_time', 'points', 'circuit_id', 'position') \
                    .withColumn('created', current_timestamp())

# COMMAND ----------

final_df = race_results_df.join(circuits_df, race_results_df.circuit_id == circuits_df.circuit_id) \
                            .select(race_results_df['*'], circuits_df.circuit_location) \
                            .drop('circuit_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verifying the data

# COMMAND ----------

final_df.filter('race_year = 2020  and race_name == "Abu Dhabi Grand Prix"').orderBy(final_df.points.desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing the file into presntation

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_path}/race_results')

# COMMAND ----------


