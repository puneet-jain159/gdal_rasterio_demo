# Databricks notebook source
# MAGIC %md
# MAGIC ### Get config values passed through 

# COMMAND ----------

# MAGIC %pip install databricks-mosaic==0.3.9 rasterio rio-cogeo

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# create the utils
dbutils.widgets.text("catalog_name", "tim_dikland")
dbutils.widgets.text("schema_name", "image")
dbutils.widgets.text("mode", "DBFS")
dbutils.widgets.text("bronze_image_location", "bronze")
bronze_tbl_name = 'bronze'
dbutils.jobs.taskValues.set(key = 'bronze_tbl_name', value = bronze_tbl_name)


# Get the values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
raw_filename = dbutils.jobs.taskValues.get(taskKey = "create_reference_file", key = "raw_filename", default = "raw",debugValue = 'raw')
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
MODE = dbutils.widgets.get("mode")
bronze_image_location = f"/dbfs/home/{username}/{catalog_name}/{schema_name}/{dbutils.widgets.get('bronze_image_location')}"
chk_point = f"dbfs:/home/{username}/{catalog_name}/{schema_name}/checkpoint/{bronze_tbl_name}"

# Set Catalogue
spark.sql (f"USE CATALOG {catalog_name}")

# COMMAND ----------

#Initialize the catalog and schema name is they do not exist
dbutils.notebook.run("./initialize_config", 300 ,{"catalog_name" : catalog_name,
                                                  "schema_name" :schema_name})

# COMMAND ----------

# Get Raw file information
df_raw = spark.readStream.table(f"{schema_name}.{raw_filename}")

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
from transform import translate_to_cog,extract_file_metadata

@udf(
    returnType=StructType(
        [
            StructField("tile", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("band", StringType(), True),
            StructField("resolution", StringType(), True),
            StructField("crs", StringType(), True),
        ]
    )
)
def extract_metadata(in_path):
    return extract_file_metadata(in_path)
  


@udf("string")
def gdal_translate_to_cog(in_path, out_prefix, out_name):
  '''
  UDF to distribute creation of COG to other formats
  '''
  out_path = str(pathlib.Path(f"{out_prefix}/{out_name}").with_suffix(".tif"))
  out_path = translate_to_cog(in_path, out_path)
  return pathlib.Path(out_path).name

# COMMAND ----------

import pathlib

if MODE == "DBFS":
    pathlib.Path(bronze_image_location).mkdir(parents=True, exist_ok=True)

df_bronze = (
    df_raw.withColumn("raw_storage_prefix", col("prefix"))
    .withColumn("raw_storage_filename", col("file_name"))
    .withColumn("bronze_storage_prefix", lit(bronze_image_location))
    .withColumn(
        "bronze_storage_filename",
        gdal_translate_to_cog(
            concat("raw_storage_prefix", lit("/"), "raw_storage_filename"),
            lit(bronze_image_location),
            "file_name",
        ),
    )
    .withColumn("bronze_image_metadata", extract_metadata("file_name"))
    .select(
        "raw_storage_prefix",
        "raw_storage_filename",
        "bronze_storage_prefix",
        "bronze_storage_filename",
        "bronze_image_metadata.tile",
        "bronze_image_metadata.timestamp",
        "bronze_image_metadata.band",
        "bronze_image_metadata.resolution",
        "bronze_image_metadata.crs",
    )
)

df_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{schema_name}.{bronze_tbl_name}")
df_bronze.display()

# COMMAND ----------

# df_bronze.writeStream \
# .outputMode("append") \
# .option("checkpointLocation", chk_point ) \
# .toTable(f"{schema_name}.{bronze_tbl_name}")

# df_bronze.display()
