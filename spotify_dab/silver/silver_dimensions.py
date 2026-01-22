# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys



# COMMAND ----------

project_path = os.path.join(os.getcwd(), '..', '..')
sys.path.append(project_path)

# COMMAND ----------

from utils.transformations import resuable

# COMMAND ----------

# MAGIC %md
# MAGIC DIM_USER

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@parneetazurestorage.dfs.core.windows.net/DimUser/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode", "rescue")\
            .load("abfss://bronze@parneetazurestorage.dfs.core.windows.net/DimUser")

            

# COMMAND ----------

df_user_obj = resuable()
df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user = df_user.withColumn("user_name", upper(col("user_name")))
df_user = (
    df_user
    .withWatermark("updated_at", "1 day")\
    .dropDuplicates(["user_id"]))




# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_catalog.silver.DimUser")
 

# COMMAND ----------

# MAGIC %md
# MAGIC DIM_ARTISRT

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@parneetazurestorage.dfs.core.windows.net/DimArtist/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode", "rescue")\
            .load("abfss://bronze@parneetazurestorage.dfs.core.windows.net/DimArtist")


# COMMAND ----------

df_artist_obj = resuable()
df_artist = df_artist_obj.dropColumns(df_artist,['_rescued_data'])
df_artist = df_artist.withColumn("artist_name", upper(col("artist_name")))
df_artist = (
    df_artist
    .withWatermark("updated_at", "1 day")\
    .dropDuplicates(["artist_id"]))


# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

spark.read.format("delta").load("abfss://silver@parneetazurestorage.dfs.core.windows.net/DimArtist/data").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC DIM_TRACK
# MAGIC

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@parneetazurestorage.dfs.core.windows.net/DimTrack/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode", "rescue")\
            .load("abfss://bronze@parneetazurestorage.dfs.core.windows.net/DimTrack")


# COMMAND ----------

df_track_obj = resuable()
df_track = df_track_obj.dropColumns(df_track,['_rescued_data'])
df_track = df_track.withColumn("track_name", upper(col("track_name")))
df_track = (
    df_track
    .withWatermark("updated_at", "1 day")\
    .dropDuplicates(["track_id"]))
df_track = df_track.withColumn("duration_flag", when(col('duration_sec') < 150, 'low')\
        .when(col('duration_sec') < 300, 'medium')\
        .otherwise('high'))

df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"),'-', ' '))


# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC DIM_DATE
# MAGIC

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@parneetazurestorage.dfs.core.windows.net/DimDate/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode", "rescue")\
            .load("abfss://bronze@parneetazurestorage.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date_obj = resuable()
df_date = df_date_obj.dropColumns(df_date,['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@parneetazurestorage.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC FACT_STREAM

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@parneetazurestorage.dfs.core.windows.net/FactStream/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode", "rescue")\
            .load("abfss://bronze@parneetazurestorage.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fact_obj = resuable()
df_fact = df_fact_obj.dropColumns(df_fact,['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@parneetazurestorage.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@parneetazurestorage.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_catalog.silver.FactStream")

# COMMAND ----------

# MAGIC %md
# MAGIC
