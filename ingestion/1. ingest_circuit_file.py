# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Circuit file for Formula 1 project

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

# MAGIC %run "../set_up/config"

# COMMAND ----------

# MAGIC %run "../set_up/common_functions"

# COMMAND ----------

#importing the datatypes
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col


# COMMAND ----------

#defining schema for my df

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), nullable= False),
                                        StructField("circuitRef", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("location", StringType(), True),
                                        StructField("country", StringType(), True),
                                        StructField("lat", DoubleType(), True),
                                        StructField("lng", DoubleType(), True),
                                        StructField("alt", DoubleType(), True),
                                        StructField("url", DoubleType(), True)

]
)

# COMMAND ----------

#reading the file
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Selecting only the required columns

# COMMAND ----------

'''circuits_selected_df = circuits_df.select(
    circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name,
    circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt
)
'''
circuits_selected_df = circuits_df.select(
    col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"),
    col("lat"), col("lng"), col("alt")
)
#display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Renaming the columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")
#display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Adding new column

# COMMAND ----------

'''#current_timestamp returns column object
#lit() helps to fill literal values in a column
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit("Prod")'''


#current_timestamp returns column object
#lit() helps to fill literal values in a column
circuits_final_df = add_ingestion(circuits_renamed_df)
#display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_path}/circuits")

# COMMAND ----------

df = spark.read.parquet(f"{processed_path}/circuits")
display(df)

# COMMAND ----------


