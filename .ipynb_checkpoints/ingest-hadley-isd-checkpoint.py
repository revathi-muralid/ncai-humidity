# Created on: 2/14/23 by RM
# Last updated: 2/14/23 by RM

# Import libraries
import awswrangler as wr
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

# https://colab.research.google.com/drive/1B7gFBSr0eoZ5IbsA0lY8q3XL8n-3BOn4#scrollTo=Z9VEsSzGrrwE

for obj in my_bucket.objects.all():
    print

dat = xr.open_zarr('s3://ncai-humidity/had-isd/hourly/720257-63835.zarr')

dat = xr.open_mfdataset

time_new = pd.to_datetime(dat.time)

dat = dat.assign_coords(time = time_new)

dat.load()

#start_time = pd.to_datetime(datetime.date(1850,1,15)) # I chose 15 for all dates to make it easier.
#time_new_hist = [start_time + pd.DateOffset(months = x) for x in range(len(ds_hist.time))]

print('dat date range  :', dat.time[0].values, ' , ', dat.time[-1].values)
# dat date range  : 2004-05-10T01:00:00.000000000  ,  2022-12-31T23:00:00.000000000

print('dat latitude range  :', dat.latitude[0].values, ' , ', dat.latitude[-1].values)
print('dat longitude range  :', dat.longitude[0].values, ' , ', dat.longitude[-1].values)

dat.dims
dat.coords
dat.variables

dat_year = dat.groupby('time.day').mean()
dat_year.temperatures.plot()
dat_year.dewpoints.plot()

myrow=0
path_to_file = "hadisd_station_info_v330_2022f.txt"

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

infile = old

ds = xr.open_dataset(infile)



ds = ds.sel(time=slice("2000-01-01", "2023-01-01"))
ds = ds.where(ds.coords["latitude"] > min_lat, drop=True)
ds = ds.where(ds.coords["latitude"] < max_lat, drop=True)
ds = ds.where(ds.coords["longitude"] > min_lon, drop=True)
ds = ds.where(ds.coords["longitude"] < max_lon, drop=True)

ds.to_zarr("s3://ncai-humidity/had-isd/hourly/" + stn_id + ".zarr", mode="w")

else:
    print('Coordinates were out of bounds!')
    variables = [
        "station_id",
        "temperatures",
        "dewpoints",
        "slp",
        "stnlp",
        "windspeeds",
        "quality_control_flags",
        "flagged_obs",
        "reporting_stats",
    ]
    ds = ds[variables]

dat.plot()
dat = dat.assign_coords

plt.figure(figsize=[12,8])
dat.plot(x='longitude', y='latitude',
              vmin=-2, vmax=32,
              cmap=cmocean.cm.thermal)

def main( startDate, endDate, bucket, variables=HDW_VARS, **kwargs ):
    """
    
    Arguments:
      startDate (datetime) : Starting date for analysis
      endDate (datetime) : Ending date for analysis
      bucket (str) : Path to S3 bucket to store output Zarr data in
      
    Returns:
      None
      
    """

    pl = pipeline(
            *paths.getAnalysisPath( variables, startDate, endDate, **kwargs ),
            **kwargs
    )
    
    if pl is not None:
      chunk = utils.getChunkData()
      return pl.assign_coords( 
        {'longitude' : chunk.longitude, 'latitude' : chunk.latitude} 
      )
    
    return None

def pipeline( varPaths, dates, tRes='1D', **kwargs ):
    """
    Build full pipeline for data injest
    
    The full pipeline for data ingest is built. This involves building
    individual pipelines for each variable, then merging them all together
    into a single pipeline.
    
    From this single, multivariable pipeline, the wind speed is computed from
    the wind components. Data are then resampled based on time (1 day) before
    a statistics function is applied. The Xarray objec for these statistics is
    returned; must run .compute() to get actual values
    
    Arguments:
      varPaths (dict) : Dictionary where keys are variable names and values are
        lists of objects on S3
      dates (list,tuple) : Datetime objects specifying dates for each of the files
        in the varPaths values.
        
    Keyword arguments:
      tRes (str) : Time resolution to bin data to using .resample() method
      All other args passed on to pipelineBase and utils.stats during .apply() call
    
    Returns:
      xarray.Dataset : Dataset pipeline ready to be run
    
    """

    ds = [pipelineBase(paths, **kwargs) for paths in varPaths.values()]    
    if all( ds ):
      ds = xr.merge( ds )
      return (
          utils.windspeed( ds )
          .reindex(  time = dates )
          .resample( time = tRes )
          .apply( utils.stats, **kwargs )
      )
    
    return None

def pipelineBase( fileGen, consolidated=None, **kwargs ):
    """
    Generate MultiFile Dataset object
    
    An xarray MultiFile Dataset is created based on the
    list of files input, which correspond to the dates input.
    
    Due to the nested layout of the HRRR Zarr files, the datetime
    data are not read in when reading just the variable data, thus
    the datetime information must be provided manually.
    
    Arguments:
        fileGen (generator) : Generator the yields (FSMap file object, datetime)
            tuples for the file/date to read

    """

    files, dates = zip( *list(fileGen) )

    if len(files) == 0:
        return None

    return (
        xr.open_mfdataset( files, 
            parallel      = True, 
            chunks        = 'auto', 
            engine        = 'zarr',
            combine_attrs = 'drop_conflicts',
            combine       = 'nested', 
            concat_dim    = [ [None]*len(files) ],
            consolidated  = consolidated
        )
        .rename( concat_dim = 'time', projection_y_coordinate = 'y', projection_x_coordinate = 'x' )
        .assign( time = np.asarray( dates, dtype='datetime64' ) )
    )
