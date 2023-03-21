# Created on: 2/14/23 by RM
# Last updated: 3/20/23 by RM

# Import libraries
import polars as pl
import pyarrow.dataset as ds
import boto3
import us
import s3fs
from functools import partial
import datetime
from datetime import date
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import json
import folium
from folium import plugins
import missingno as msno
import seaborn as sns
import xarray as xr
import numpy as np
import zarr

s3 = s3fs.S3FileSystem(anon=False)
s3path = 's3://ncai-humidity/had-isd/hourly/7*'
remote_files = s3.glob(s3path)

# Iterate through remote_files to create a fileset
dataset_names = remote_files
fileset = [f"s3:///{dataset_name}" for dataset_name in dataset_names]

# Open one dataset to get dims
# test = xr.open_dataset(fileset[10], engine='zarr')
# test1 = xr.open_dataset(fileset[11], engine='zarr',decode_coords='time')

# Get filename since there's something wrong with the way station_id files are being read in
# Write function to do this

def get_fname(ds):
    print(ds.encoding['source'])
    filename=ds.encoding['source'][ds.encoding['source'].rindex('/')+1:]
    ds['station_id']=filename.split('.')[0]
    return ds

# open_mf on two files to test get_fname function

# test_df = xr.open_mfdataset(fileset[10:12],engine='zarr',decode_coords='time',concat_dim="time",combine="nested",preprocess=get_fname)

# Frozen({'time': 149752, 'coordinate_length': 1, 'flagged': 19, 'test': 71, 'reporting_v': 19, 'reporting_t': 1104, 'reporting_2': 2})

# Function works! Now get all Had-ISD files

data = xr.open_mfdataset(fileset,engine='zarr',combine="nested",preprocess=get_fname,concat_dim="time", decode_coords="time")
# concat_dim="time", decode_coords="time"
#df = data.to_dataframe() -- this is 14.3 PiB! Nope!

# Treat NAs
# The netCDF structure automatically has defined a missing data indicator (MDI) which has been set to −1×1030. Where the QC tests have set flags and those values have been removed, they are replaced by a flagged data indicator (FDI) of −2 ×1030.

ds = data.where(data['temperatures'] != -2e+30)
ds2 = ds.where(ds['dewpoints'] != -2e+30)
ds3 = ds2.where(ds2['windspeeds'] != -2e+30)
ds_masked = ds3.where(ds3['stnlp'] != -2e+30)

# Drop variables with odd dimensions for outputting purposes

ds_out = ds_masked.drop_vars(["reporting_stats","flagged_obs","quality_control_flags"])

dsf = ds_out.squeeze("coordinate_length")

### Add RH column

# ds_masked.assign(rh_num=lambda ds_masked: math.exp(17.625 * ds_masked.dewpoints/(243.04 + ds_masked.dewpoints)))

# ds_masked.assign(rh_den=lambda ds_masked: math.exp(17.625 * ds_masked.temperatures/(243.04 + ds_masked.temperatures)))

# ds_masked.assign(rh=lambda ds_masked: 100*(ds_masked.rh_num/ds_masked.rh_den))

dsf.to_pandas().to_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL.parquet')

### CHECK FILE







# data.groupby('station_id').groups
# 6071711,6071712,6071713,6071714
# 212 stations

# Code from: https://stackoverflow.com/questions/69784076/xarray-groupby-according-to-multi-indexs

#start_time = pd.to_datetime(datetime.date(1850,1,15)) # I chose 15 for all dates to make it easier.
#time_new_hist = [start_time + pd.DateOffset(months = x) for x in range(len(ds_hist.time))]

# print('dat date range  :', dat.time[0].values, ' , ', dat.time[-1].values)
# dat date range  : 2004-05-10T01:00:00.000000000  ,  2022-12-31T23:00:00.000000000

# print('dat latitude range  :', dat.latitude[0].values, ' , ', dat.latitude[-1].values)
# print('dat longitude range  :', dat.longitude[0].values, ' , ', dat.longitude[-1].values)

# dat.dims
# dat.coords
# dat.variables

min_lon = -94.617919
min_lat = 24.523096
max_lon = -75.242266
max_lat = 39.466012

df = pd.read_table(path_to_file, sep="\s+", header=None)
new_df = df[~df[0].str.contains("99999")]
stations = new_df[new_df[0].str[0:1].isin(["7"])] # 1468 stations
stations = stations[stations[1]>min_lat] # 1458
stations = stations[stations[1]<max_lat] # 740
stations = stations[stations[2]>min_lon] # 401
stations = stations[stations[2]<max_lon] # 396

stn_id = stations.iloc[myrow][0]
url = (
    "https://www.metoffice.gov.uk/hadobs/hadisd/v330_2022f/data/hadisd.3.3.0.2022f_19310101-20230101_"
    + stn_id
    + ".nc.gz"
)
new = "/tmp/" + stn_id + ".nc.gz"

urllib.request.urlretrieve(url, new)
old = "/tmp/" + stn_id + ".nc"

with gzip.open(new, "rb") as f_in:
    with open(old, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)





ds = ds.sel(time=slice("2000-01-01", "2023-01-01"))
ds = ds.where(ds.coords["latitude"] > min_lat, drop=True)
ds = ds.where(ds.coords["latitude"] < max_lat, drop=True)
ds = ds.where(ds.coords["longitude"] > min_lon, drop=True)
ds = ds.where(ds.coords["longitude"] < max_lon, drop=True)

