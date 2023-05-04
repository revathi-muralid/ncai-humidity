# Created on: 2/14/23 by RM
# Last updated: 4/28/23 by RM

# Import libraries
import polars as pl
import pyarrow.dataset as ds
import boto3
import s3fs
import datetime
from datetime import date
import pandas as pd
import xarray as xr
import numpy as np
import zarr
import reverse_geocoder as rgcr
import dask.dataframe as dd

s3 = s3fs.S3FileSystem(anon=False)
s3path = 's3://ncai-humidity/had-isd/hourly/pq/*'
remote_files = s3.glob(s3path)

# Iterate through remote_files to create a fileset
dataset_names = remote_files
fileset = [f"s3://{dataset_name}" for dataset_name in dataset_names[315:396]]

queries = []
for file in fileset:
    q = (
        ds.dataset(file, partitioning='hive')
    )
    queries.append(q)
    
isd = (
    pl.from_arrow(
        ds.dataset(queries)
        .to_table()
    )
    #.filter(pl.col('time') >= pl.datetime(2000,1,1))
)

isd.to_pandas().to_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL_pt2.parquet')

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

#data = xr.open_mfdataset(fileset,engine='zarr',combine="nested",preprocess=get_fname,concat_dim="time", decode_coords="time")
# concat_dim="time", decode_coords="time"
#df = data.to_dataframe() -- this is 14.3 PiB! Nope!

# Treat NAs
# The netCDF structure automatically has defined a missing data indicator (MDI) which has been set to −1×1030. Where the QC tests have set flags and those values have been removed, they are replaced by a flagged data indicator (FDI) of −2 ×1030.

# Drop variables with odd dimensions for outputting purposes

#ds_out = ds_masked.drop_vars(["reporting_stats","flagged_obs","quality_control_flags"])

#dsf = ds_out.squeeze("coordinate_length")

### Add RH column

# ds_masked.assign(rh_num=lambda ds_masked: math.exp(17.625 * ds_masked.dewpoints/(243.04 + ds_masked.dewpoints)))

# ds_masked.assign(rh_den=lambda ds_masked: math.exp(17.625 * ds_masked.temperatures/(243.04 + ds_masked.temperatures)))

# ds_masked.assign(rh=lambda ds_masked: 100*(ds_masked.rh_num/ds_masked.rh_den))

dsf.to_pandas().to_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL.parquet')

######################  CLEAN HADLEY ISD DATA ###################### 

hadley = pl.scan_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL_pt1.parquet')
hadley2 = pl.scan_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL_pt2.parquet')
had_locs = hadley.select(['stn_id','lon','lat']).unique().collect()
had_locs2 = hadley2.select(['stn_id','lon','lat']).unique().collect()

had_locs = had_locs.to_pandas()
had_locs2 = had_locs2.to_pandas()

hadley = hadley.reset_index()

# Get state name using reverse geocoder

state=[]
for i in range(len(had_locs)):
    results = rgcr.search((had_locs['lat'][i],had_locs['lon'][i]))
    state.append(results[0]['admin1'])
    
state2=[]
for i in range(len(had_locs2)):
    results = rgcr.search((had_locs2['lat'][i],had_locs2['lon'][i]))
    state2.append(results[0]['admin1'])

had_locs['state'] = state
had_locs2['state'] = state2

# Remove Maryland, Delaware, Indiana, Missouri, West Virginia, Illinois, Ohio, Texas records

excludes = ["Maryland", "Delaware", "Indiana", "Missouri", "West Virginia", "Illinois", "Ohio", "Texas"]

had_locs = had_locs[~had_locs['state'].isin(excludes)]
had_locs2 = had_locs2[~had_locs2['state'].isin(excludes)]

frames = [had_locs, had_locs2]
all_hlocs = pd.concat(frames)
all_hlocs = all_hlocs.reset_index()
all_hlocs = all_hlocs.drop('index',axis=1)

all_hlocs.to_parquet('s3://ncai-humidity/had-isd/hadley_stations.parquet')

stn_list = list(all_hlocs['stn_id'])

hadf = hadley.filter(pl.col("stn_id").is_in(stn_list))
hadf2 = hadley2.filter(pl.col("stn_id").is_in(stn_list))

hadley.select(pl.count()).collect() #56,157,249
hadf.select(pl.count()).collect() #51,291,995


hadley2.select(pl.count()).collect() #14,257,704
hadf2.select(pl.count()).collect() #7,517,914

hadf.collect().to_pandas().to_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_pt1_clean.parquet')
hadf2.collect().to_pandas().to_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_pt2_clean.parquet')
