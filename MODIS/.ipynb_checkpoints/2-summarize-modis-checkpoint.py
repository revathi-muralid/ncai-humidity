
# Created on: 4/11/23 by RM
# Last updated: 4/24/23 by RM

import pandas as pd
import os
import awswrangler as wr
import s3fs
import boto3
from numpy import *
import numpy as np
import pyarrow.dataset as ds
from datetime import datetime
from datetime import timedelta
import json
import folium
from folium import plugins
from folium.features import DivIcon
from aws_s3_nfiles import aws_s3_nfiles
import polars as pl
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.fs as pa_fs

prefix="HIRS/SE_HIRS"

# Scan in station data

fnames = aws_s3_nfiles("ncai-humidity",".parquet","MODIS/MOD07_L2")
files = fnames[1:len(fnames)]

files = ["s3://ncai-humidity/MODIS/MOD07_L2/" + f for f in files]

range(len(files))

df = pd.read_parquet("s3://ncai-humidity/MODIS/MOD07_L2/"+files[0])

ddf = dd.read_parquet(files)

ddf.groupby(ddf.Scan_Start_Time).Retrieved_Temperature_Profile.mean()


ddf = dd.read_parquet(
    "s3://ncai-humidity/MODIS/MOD07_L2/"+files,
    engine="pyarrow",
    storage_options={"anon": False},
    open_file_options={
        "open_file_func": fs.open_input_file,
    },
)

ddf.partitions[0].compute()

queries = []
for myfile in files:
    q = (
        ds.dataset("s3://ncai-humidity/MODIS/MOD07_L2/"+myfile,partitioning="hive")
    )
    queries.append(q)
    
test=pd.read_parquet("s3://ncai-humidity/MODIS/MOD07_L2/MODIS/MOD07_L2/MOD07_L2.A2000055.1645.061.2017202185147.parquet")

### Read in example dataset to get colnames    

dat = wr.s3.read_parquet(
    path="s3://ncai-humidity/MODIS/MOD07_L2/"+files[1]
)

for col in dat.columns:
    print(col)

### Read in all years and states into one df
    
myvars = ["Latitude", "Longitude", "Scan_Start_Time", "Cloud_Mask", "Surface_Pressure",
"Surface_Elevation","Processing_Flag","Retrieved_Temperature_Profile",
          "Retrieved_Moisture_Profile","Water_Vapor","Water_Vapor_Low",
          "Water_Vapor_High","QA"]
    
df = (
    pl.from_arrow(
        pl.collect_all(queries)
        .select(myvars)
     )
)    

df=df.rename({"('Control', 'datetime')":"datetime","('Control', 'USAF')":"USAF_ID",
"('Control', 'WBAN')":"WBAN_ID","('Control', 'latitude')":"lat","('Control', 'longitude')":"long","('Control', 'elevation')":"elev"})
