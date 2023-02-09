"""
Utilities for file path/URL/URI manipulation and creation.

Usefull for keeping paths to data/figures/etc. consistent
across the code base.
"""
import logging
import threading

from datetime import datetime, timedelta
from functools import partial
from concurrent.futures import ThreadPoolExecutor

import boto3
import numpy as np

from ... import HRRRZARR, OUTPUT_BUCKET
from ...utils.aws import folder_exists, obj_exists
from ...utils.dates import genDates, nextMonth

URL = f"s3://{HRRRZARR}/"

def hadISDFiles( 
      startDate  = datetime(2020, 1,  1, 0), 
      endDate    = datetime(2020, 2,  1, 0),
      bucketName = OUTPUT_BUCKET
    ):
    """
    Generate list of Hadley ISD files

    As HRRR data are large, preprocessed files were created that contain
    daily statistics for variables of interest at each grid point.
    These statistics may include minimum, maximum, median, average, etc.
    values for given day at a given grid box. Data are stored in monthly
    files in an bucket on AWS S3.

    Given a range of dates, this function generates URIs to the NetCDF4 files
    containing the hourly Hadley ISD data.

    Keyword arguments:
        startDate (datetime) : Starting datetime [inclusive] of period
            of interest.
        endDate (datetime) : Ending datetime [exclusive] of period
            of interest.
        bucketName (str) : Name of bucket where data are located.

    Returns:
        list : URIs to monthly preproccessed HRRR files.

    """

    files = []
    for sDate in genDates( startDate, endDate, months=1 ):
        fname = f"hrrr_hdwi_inputs_{sDate:%Y%m}.zarr"
        files.append( f"s3://{bucketName}/{fname}" )
    return files

class HRRRZarrPathBuilder( object ):
    """
    Robust class for generation of HRRR Zarr file paths

    This class will generate path(s) to HRRR Zarr data files
    stored in an AWS S3 bucket via a simple API. Initialize
    the class using the level and variable of interest, and
    then use the various methods to generate a single, or
    range, of path(s) given a date, or dates, of interest.
    """

    RESOURCE = boto3.resource( 's3' )

    def __init__(self, level, variable, prefix=URL, **kwargs):
        """
        Initialize the class

        Arguments:
            level (str) : Level of interest
            variable (str) : Variable of interest

        Keyword arguments:
            prefix (str) : Prefix to the S3 bucket
            **kwargs : All other kwargs silently ignored

        """

        self.level      = level
        self.variable   = variable
        self.prefix     = prefix

        self._resources = {}
        self._startDate = None
        self._endDate   = None
        self._func      = None
        self._pool      = None
        self._paths     = None

    @staticmethod
    def _setDates( func ):
        def wrapped( self, *args ):
          self._startDate = args[-2]
          self._endDate   = args[-1]
          return func( self, *args )

        return wrapped

    def __iter__(self):
        """
        Run when trying to get list or iterate over object

        When this is called, submit parallel path build/check
        to the pool.

        """

        self._pool  = ThreadPoolExecutor( initializer = self._threadInit )
        self._paths = self._pool.map( 
            self._func, 
            genDates( 
                self._startDate, self._endDate, hours=1
            )
        )
        return self

    def __next__(self):
        """
        Get next path out of pool

        When method called, will search for the next
        not-None data returned by the pool

        """

        path = None
        while path is None:
            try:
              path = next( self._paths )
            except StopIteration:
              self._cleanUp()
              raise StopIteration
        return path 

    def _threadInit(self):
        """
        Initializer function for threads in thread pool
    
        Initialize a thread specific boto3 resources (and sessions)
        object by placing reference in a global dictionary under a
        key matching the thread's identifier
    
        """
    
        tid = threading.get_ident()
        if tid not in self._resources:
            self._resources[tid] = boto3.session.Session().resource('s3')
    
    def _cleanUp(self):
        """Clean up resource references and the pool"""
    
        self._resources = {}
        if self._pool is not None:
            self._pool.shutdown()
            self._pool = None 

         
    def _path( self, init, valid=None ):
        """
        Get path to hrrrzarr object 
        
        Arguments:
          init (datetime) : Initialization date for model run
   
        Keyword arguments:
          valid (datetime) : Valid date for forecast.
              If missing or same as init, then will use
              analysis
            
        Returns:
          None,tuple : If the data does not exist, then None is
              returned. Else, a tuple of the object prefix and
              analysis date are returned
          
        """
    
        resource = self._resources.get( 
            threading.get_ident(),
            self.RESOURCE
        )
    
        if not isinstance( valid, datetime ):
            valid = init 
    
        dt    = (valid - init).total_seconds() // 3600
        dinfo = f"{dt:02.0f}z_fcst" if dt > 0.0 else f"{init:%H}z_anl"
        fname = f"{init:%Y%m%d}_{dinfo}.zarr"
        key   = f"sfc/{init:%Y%m%d}/{fname}/{self.level}/{self.variable}/{self.level}"
    
        if folder_exists( resource, HRRRZARR, key ) and obj_exists( resource, HRRRZARR, f"{key}/.zgroup" ):
            return f"{self.prefix}{key}", init 
        return None
    
    def analysis( self, init ):
        """Get path for single analysis time"""

        return self._path( init )

    def forecast( self, init, valid ):
        """Get path for single forecast time"""

        return self._path( init, valid )

    @_setDates
    def analysisRange( self, startDate, endDate ):
        """Get paths for range of analysis times"""

        self._func = self._path 
        return self

    @_setDates
    def forecastRange( self, init, startDate, endDate ):
        """Get paths for range of forecast times"""

        self._func = partial( self._path, init ) 
        return self

def getAnalysisPath( variables, startDate, endDate, **kwargs ):
    """
    Build dict of analysis path generators
    
    This function will build a generateAnalysisPaths() generator for each of the
    level/variable pairs in the variables input dictionary. These generators will
    yield FSMap and datetime objects for every analysis time that exists in the 
    HRRRZARR S3 bucket. These generator objects are stored in a dictionary under
    keys created using the {level}-{variable} information.
    
    On return, this function gives the dict of all key/datetime generators and a
    numpy datetime64 array with all the times that SHOULD exist in all variables.
    This datetime64 array can be used to reindex (fill) missing datetimes.
    
    Arguments:
        variables (dict) : Dictionary where keys are vertical levels and values
            are lists of variables on that level (key) to be read
        startDate (datetime) : Starting date to read in
        endDate (datetime) : Ending date to read in; NOT inclusive
    
    Keyword arguments:
        All keywords are passed to the genAnalysisVarPaths() generator
    
    Returns:
        tuple : Dict, datetime64
    
    """

    urls = {}
    for lvl, var in variables.items():
        for v in var:
            gen = HRRRZarrPathBuilder(lvl, v, **kwargs)
            urls[f"{lvl}-{v}"] = gen.analysisRange( startDate, endDate )
 
    return urls, np.arange( startDate, endDate, timedelta( hours=1 ) )

import os 
import matplotlib.pyplot as plt
import pandas as pd
import requests
import urllib
import netCDF4
from netCDF4 import Dataset, num2date
import gzip
import tempfile
import shutil
import xarray as xr
import numpy.ma as ma
import zarr
import s3fs
from s3fs import S3Map, S3FileSystem

# Get US station IDs from HadISD station list
path_to_file = 'hadisd_station_info_v330_2022f.txt'
    
df = pd.read_table(path_to_file, sep="\s+", header=None)
new_df = df[~df[0].str.contains("99999")]
stations = new_df[new_df[0].str[0:1].isin(['7'])]

def getHadISDData(myrow):
    """
    
    This function will build a generateAnalysisPaths() generator for each of the
    level/variable pairs in the variables input dictionary. These generators will
    yield FSMap and datetime objects for every analysis time that exists in the 
    HRRRZARR S3 bucket. These generator objects are stored in a...
    
    On return, this function gives the station ID of the Hadley ISD station data was 
    downloaded for.
    
    Arguments:
        myrow (int) : Row number of station in the list of stations
    
    Returns:
        stn_id : string
    
    """
    
    stn_id = stations.iloc[myrow][0]
    url = "https://www.metoffice.gov.uk/hadobs/hadisd/v330_2022f/data/hadisd.3.3.0.2022f_19310101-20230101_"+stn_id+".nc.gz"
    new = stn_id+".nc.gz"
    urllib.request.urlretrieve(url, new)
    old = stn_id+".nc"

    with gzip.open(new, 'rb') as f_in:
        with open(old, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    infile = old
    
    ds = xr.open_dataset(infile)
    
    ds = ds.sel(time=slice('2000-01-01', '2023-01-01'))
    
    variables= [
    'station_id',
     'temperatures',
     'dewpoints',
     'slp',
     'stnlp',
     'windspeeds',
     'quality_control_flags',
     'flagged_obs',
     'reporting_stats'
    ]
    ds = ds[variables]
    
    #ds.to_zarr(store= s3fs.S3Map(root=f's3://ncai-humidity/had-isd/hourly/'+stn_id+'.zarr', s3=s3 ,check=False))
    
    ds.to_zarr('s3://ncai-humidity/had-isd/hourly/'+stn_id+'.zarr',mode='w')
    
    os.remove(infile)
    os.remove(new)
 
    return stn_id
    
    

myrows = range(0,1)

[getHadISDData(x) for x in myrows]


    
stn_id = stations.iloc[myrow][0]
url = "https://www.metoffice.gov.uk/hadobs/hadisd/v330_2022f/data/hadisd.3.3.0.2022f_19310101-20230101_"+stn_id+".nc.gz"
new = stn_id+".nc.gz"
urllib.request.urlretrieve(url, new)
old = stn_id+".nc"

with gzip.open(new, 'rb') as f_in:
    with open(old, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

infile = old
ds = xr.open_dataset(infile)



#ds.to_zarr(store= s3fs.S3Map(root=f's3://ncai-humidity/had-isd/hourly/'+stn_id+'.zarr', s3=s3 ,check=False))
ds.to_zarr('s3://ncai-humidity/had-isd/hourly/'+stn_id+'.zarr',mode='w')
    
os.remove(infile)
    
