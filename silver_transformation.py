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
dbutils.widgets.text("silver_image_location", "silver")
silver_tbl_name = 'silver'
dbutils.jobs.taskValues.set(key = 'silver_file_name', value = silver_tbl_name)


# Get the values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
bronze_tbl_name = dbutils.jobs.taskValues.get(taskKey = "bronze_transformation", key = "bronze_tbl_name", default = "bronze",debugValue = 'bronze')
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
MODE = dbutils.widgets.get("mode")
silver_image_location = f"/dbfs/home/{username}/{catalog_name}/{schema_name}/{dbutils.widgets.get('silver_image_location')}"
dbutils.jobs.taskValues.set(key = 'silver_image_location', value = silver_image_location)
# Set Catalogue
spark.sql (f"USE CATALOG {catalog_name}")

# COMMAND ----------

#Initialize the catalog and schema name is they do not exist
dbutils.notebook.run("./initialize_config", 300 ,{"catalog_name" : catalog_name,
                                                  "schema_name" :schema_name})

# COMMAND ----------

# Get Bronze file information
df_bronze = spark.read.table(f"{schema_name}.{bronze_tbl_name}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### create udf

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pathlib
from transform import transform_raster_crs

@udf("string")
def transform_raster_crs_espg_4326(in_path, out_path):
    out = str(pathlib.Path(out_path).with_suffix(".tif"))
    return transform_raster_crs(in_path, out, "EPSG:4326")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Spatial Transformation

# COMMAND ----------

if MODE == "DBFS":
    pathlib.Path(silver_image_location).mkdir(parents=True, exist_ok=True)

df_silver = (
    df_bronze.withColumn("silver_storage_prefix", lit(silver_image_location))
    .withColumn("silver_storage_filename", col("bronze_storage_filename"))
    .withColumn(
        "silver_storage_path",
        transform_raster_crs_espg_4326(
            concat("bronze_storage_prefix", lit("/"), "bronze_storage_filename"),
            concat(lit(silver_image_location), lit("/"), "bronze_storage_filename"),
        ),
    )
    .withColumn("crs", lit("EPSG:4326"))
    .select(
        "bronze_storage_prefix",
        "bronze_storage_filename",
        "silver_storage_prefix",
        "silver_storage_filename",
        "silver_storage_path",
        "tile",
        "band",
        "crs",
    )
)

df_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{schema_name}.{silver_tbl_name}")
df_silver.display()

# COMMAND ----------


