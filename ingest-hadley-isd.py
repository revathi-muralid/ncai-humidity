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
from datetime import datetime
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

myyr = 2000

dat = xr.open_zarr('s3://ncai-humidity/had-isd/hourly/700001-26492.zarr')

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
