# Databricks notebook source
# MAGIC %pip install databricks-mosaic==0.3.9 rasterio rio-cogeo

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get config values passed through 

# COMMAND ----------

# create the utils
dbutils.widgets.text("catalog_name", "tim_dikland")
dbutils.widgets.text("schema_name", "image")
dbutils.widgets.text("mode", "DBFS")
dbutils.widgets.text("gold_image_location", "gold")
gold_tbl_name = 'gold'
dbutils.jobs.taskValues.set(key = 'gold_tbl_name', value = gold_tbl_name)


# Get the values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
silver_tbl_name = dbutils.jobs.taskValues.get(taskKey = "silver_transformation", key = "silver_tbl_name", default = "silver",debugValue = 'silver')
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
MODE = dbutils.widgets.get("mode")
gold_image_location = f"/dbfs/home/{username}/{catalog_name}/{schema_name}/{dbutils.widgets.get('gold_image_location')}"
silver_image_location = dbutils.jobs.taskValues.get(taskKey = "silver_transformation", key = "silver_image_location", 
                                                    default = "/dbfs/home/puneet.jain@databricks.com/tim_dikland/image_puneet/silver/",
                                                    debugValue = "/dbfs/home/puneet.jain@databricks.com/tim_dikland/image_puneet/silver/")

# Set Catalogue
spark.sql (f"USE CATALOG {catalog_name}")

# COMMAND ----------

#Initialize the catalog and schema name is they do not exist
dbutils.notebook.run("./initialize_config", 300 ,{"catalog_name" : catalog_name,
                                                  "schema_name" :schema_name})

# COMMAND ----------

# Get Bronze file information
df_silver = spark.read.table(f"{schema_name}.{silver_tbl_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Preparing consumption ready datasets
# MAGIC Make sure that downstream applications can efficiently use the produced datasets.
# MAGIC - e.g. collect bands into a multispectral image.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pathlib
import pandas as pd
from transform import collect_to_rgb

@pandas_udf(
    returnType=StructType(
        [
            StructField("gold_storage_path", StringType(), True),
            StructField("silver_storage_paths", ArrayType(StringType()), True),
        ]
    ),
    functionType=PandasUDFType.GROUPED_MAP,
)
def collect_to_rgb_map(data: pd.DataFrame) -> pd.DataFrame:
  return collect_to_rgb(data,
                        gold_image_location = gold_image_location,
                        silver_storage_location = silver_image_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Spatial Transformation

# COMMAND ----------

if MODE == "DBFS":
    pathlib.Path(gold_image_location).mkdir(parents=True, exist_ok=True)

df_gold = (
    df_silver.filter(col("band").isin(["B02", "B03", "B04"]))
    .withColumn(
        "silver_storage_paths",
        concat("silver_storage_prefix", lit("/"), "silver_storage_filename"),
    )
    .groupBy("tile")
    .apply(collect_to_rgb_map)
)

df_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{schema_name}.{gold_tbl_name}"
)
df_gold.display()

# COMMAND ----------


