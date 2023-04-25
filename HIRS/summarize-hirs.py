# Created on: 4/24/23 by RM
# Last updated: 4/24/23 by RM

import xarray as xr
import zarr
import s3fs
import numpy as np
import pyarrow
import os
from aws_s3_nfiles import aws_s3_nfiles
import pyarrow.dataset as ds

s3 = s3fs.S3FileSystem(anon=False)
s3path = 's3://ncai-humidity/HIRS/SE_HIRS*'
remote_files = s3.glob(s3path)

# Iterate through remote_files to create a fileset
dataset_names = remote_files
fileset = [f"s3:///{dataset_name}" for dataset_name in dataset_names]

data = xr.open_dataset(fileset[1],engine='zarr')
d2 = xr.open_dataset(fileset[2],engine='zarr')

data = xr.open_mfdataset(fileset[0:2],engine='zarr',combine="nested",concat_dim=None,
                        decode_coords=None)

dat = data.merge(d2,compat='no_conflicts')

data=xr.auto_merge(fileset[0:2])
