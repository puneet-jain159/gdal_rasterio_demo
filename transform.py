import subprocess
import tempfile
import shutil
import re
import os
import pathlib
from datetime import datetime

import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *


from affine import Affine
import rasterio as rio
from rasterio.crs import CRS
from rasterio.warp import calculate_default_transform, Resampling, reproject
from rasterio.io import MemoryFile
from rasterio.plot import show


def translate_to_cog(in_path, out_path,block_size = '128'):
  '''
  Functions to convert the file to COG
  '''
  with tempfile.NamedTemporaryFile() as tmp_file:
      subprocess.run(
          [
              "gdal_translate",
              "-of",
              "COG",
              "-co",
              f"BLOCKSIZE={block_size}",
              "-co",
              "COMPRESS=DEFLATE",
              "-co",
              "NUM_THREADS=ALL_CPUS",
              in_path,
              tmp_file.name
          ]
      )
      shutil.copyfile(tmp_file.name, out_path)
  return out_path


def extract_file_metadata(in_path):
    '''
    Extract Metadata from the file name
    '''
    regex = r"^([A-Z\d]+)_(\d{8}T\d{6})_([A-Z\d]+)_([\d]+[mM])\.jp2$"
    matches = re.match(regex, in_path)
    if matches:
        groups = matches.groups()
        return {
            "tile": groups[0],
            "timestamp": datetime.strptime(groups[1], "%Y%m%dT%H%M%S"),
            "band": groups[2],
            "resolution": groups[3],
            "crs": "EPSG:32632",
        }
    else:
        return {
            "tile": None,
            "timestamp": None,
            "band": None,
            "resolution": None,
            "crs": None,
        }


def transform_raster_crs(in_path, out_path, target_crs="EPSG:4326"):
    with rio.open(in_path) as src:
        profile = src.profile.copy()
        transform, width, height = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, *src.bounds
        )
        with tempfile.NamedTemporaryFile() as tmp_file:
            with rio.open(
                tmp_file.name,
                mode="w+",
                driver="GTiff",
                count=1,
                crs=target_crs,
                transform=transform,
                width=width,
                height=height,
                dtype=profile["dtype"],
            ) as dst:
                reproject(
                    source=rio.band(src, 1),
                    destination=rio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst.transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest,
                )
            shutil.copyfile(tmp_file.name, out_path)
        return out_path

def scale_to_rgb(band ,threshold: str = 0.2):
    scaled_band = np.clip(band / 10000, 0, 1)
    deskewed_band = np.clip(scaled_band, 0, threshold) / threshold
    return 1 + np.clip((deskewed_band * 254) - 140, 0, 254)
  
def collect_to_rgb(data: pd.DataFrame,
                   gold_image_location: str,
                   silver_storage_location:str,
                   ref_image:str = 'T32UME_20230419T102601_B04_60m.tif'
                   ) -> pd.DataFrame:
    tile = data.loc[0, "tile"]
    out_path = f"{gold_image_location}/{tile}_rgb.tif"
    files = data.loc[:, "silver_storage_path"].tolist()

    with rio.open(
        os.path.join(silver_storage_location, ref_image)
    ) as src:
        profile = src.profile

    with tempfile.NamedTemporaryFile() as tmp_file:
        dst_idx = 1
        with rio.open(
            tmp_file.name,
            mode="w+",
            driver="GTiff",
            count=3,
            crs=profile["crs"],
            transform=profile["transform"],
            width=profile["width"],
            height=profile["height"],
            dtype=profile["dtype"],
        ) as dst:
            for p in files:
                with rio.open(p) as src:
                    data = scale_to_rgb(src.read(1))
                    dst.write(data, dst_idx)
                    dst_idx += 1
        shutil.copyfile(
            tmp_file.name,
            out_path,
        )

    return pd.DataFrame.from_dict(
        {
            "gold_storage_path": [out_path],
            "silver_storage_paths": [files],
        }
    )

