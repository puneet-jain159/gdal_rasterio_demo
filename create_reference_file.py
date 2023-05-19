# Databricks notebook source
# MAGIC %md
# MAGIC ### Create a base table referencing the raw files and the storage location

# COMMAND ----------

# create the utils
dbutils.widgets.text("catalog_name", "tim_dikland")
dbutils.widgets.text("schema_name", "image")
dbutils.widgets.text("raw_image_location", "dbfs:/FileStore/geospatial/jp2")
# set raw filename 
raw_filename = 'raw'
dbutils.jobs.taskValues.set(key = 'raw_filename', value = raw_filename)

# Get the values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
raw_image_location = dbutils.widgets.get("raw_image_location")
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
chk_point = f"dbfs:/home/{username}/{catalog_name}/{schema_name}/checkpoint/raw"

# Set Catalogue
spark.sql (f"USE CATALOG {catalog_name}")

# COMMAND ----------

# %run ./initialize_config $catalog_name=catalog_name $database_name=database_name
dbutils.notebook.run("./initialize_config", 300 ,{"catalog_name" : catalog_name,
                                                  "schema_name" :schema_name})

# COMMAND ----------

raw_source_paths = [
    "T32UME_20230419T102601_AOT_60m.jp2",
    "T32UME_20230419T102601_B01_60m.jp2",
    "T32UME_20230419T102601_B02_60m.jp2",
    "T32UME_20230419T102601_B03_60m.jp2",
    "T32UME_20230419T102601_B04_60m.jp2",
    "T32UME_20230419T102601_B05_60m.jp2",
    "T32UME_20230419T102601_B06_60m.jp2",
    "T32UME_20230419T102601_B07_60m.jp2",
    "T32UME_20230419T102601_B09_60m.jp2",
    "T32UME_20230419T102601_B11_60m.jp2",
    "T32UME_20230419T102601_B12_60m.jp2",
    "T32UME_20230419T102601_B8A_60m.jp2",
    "T32UME_20230419T102601_SCL_60m.jp2",
    "T32UME_20230419T102601_TCI_60m.jp2",
    "T32UME_20230419T102601_WVP_60m.jp2",
]

raw_df = spark.createDataFrame(
    [("/dbfs/FileStore/geospatial/jp2", p) for p in raw_source_paths], "prefix STRING, file_name STRING"
)
raw_df.display()

# COMMAND ----------

raw_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{schema_name}.{raw_filename}")

# COMMAND ----------

# from pyspark.sql.functions import *

# spark.readStream.format("cloudFiles") \
#   .option("cloudFiles.format", "binaryFile") \
#   .load("dbfs:/FileStore/geospatial/jp2") \
#   .withColumn("p", split(col('path'), '/'))\
#   .withColumn("file_name",element_at(col('p'), -1))\
#   .withColumn("prefix", lit(raw_image_location.replace('dbfs:','/dbfs')))\
#   .select('prefix','file_name')\
#   .writeStream \
#   .option("checkpointLocation", chk_point) \
#   .trigger(once=True) \
#   .toTable(f"{schema_name}.{raw_filename}")

# COMMAND ----------

# MAGIC # %sql
# MAGIC # Drop table image_puneet.raw
