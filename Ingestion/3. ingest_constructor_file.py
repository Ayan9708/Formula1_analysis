# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesting Constructors

# COMMAND ----------

# MAGIC %md
# MAGIC ###Requirements
# MAGIC ###### 1. Read the json file
# MAGIC ###### 2. Transform the data
# MAGIC ###### 3. Write the data in parquet file

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#defining schema

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality String, url STRING"

# COMMAND ----------

#reading the json file

constructors_df = spark.read \
    .schema(constructors_schema) \
    .json('/mnt/udemystrg/raw/raw/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Dropping redundant columns

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')
#constructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Renaming Columns and adding ingestion time column

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
                                                .withColumnRenamed('constructorRef', 'constructor_ref') \
                                                .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet('/mnt/udemystrg/processed/constructors')
