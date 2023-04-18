
# Created on: 4/11/23 by RM
# Last updated: 4/11/23 by RM

import pandas as pd
import os
import awswrangler as wr
import s3fs
import boto3
from numpy import *
import numpy as np
from datetime import datetime
from datetime import timedelta
import xarray as xr
import zarr

fname = 'MOD07_L2.A2000153.1640.061.2017207224921'
test = pd.read_parquet("s3://ncai-humidity/MODIS/MOD07_L2/" + fname + ".parquet")

### HIRS

hirname = 's3://ncai-humidity/HIRS/SE_HIRS_AtmosphericProfiles_V5_M02_2007364_int.zarr/'

# Initilize the S3 file system
s3 = s3fs.S3FileSystem()
store = s3fs.S3Map(root=hirname, s3=s3, check=False)
# Read Zarr file
ds = xr.open_zarr(store=store, consolidated=True)

ds = xr.open_dataset([s3fs.S3Map(hirname, s3=fs) for url in [group_url, subgroup_url]], engine='zarr')

test = xr.open_zarr(store)
                       #,concat_dim="time", decode_coords="time")
