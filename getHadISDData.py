
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
import awswrangler as wr

# Get US station IDs from HadISD station list
path_to_file = 'hadisd_station_info_v330_2022f.txt'
    
df = pd.read_table(path_to_file, sep="\s+", header=None)
new_df = df[~df[0].str.contains("99999")]
stations = new_df[new_df[0].str[0:1].isin(['7'])]

# Get bounding box

# NW corner: 36.4996,-94.617919
# NE corner: 39.466012,-75.242266
# SW corner: 28.928609,-94.043147
# SE corner: 24.523096,-80.031362
# AR | Arkansas | -94.617919 | 33.004106 | -89.644395 | 36.4996 |
# VA | Virginia | -83.675395 | 36.540738 | -75.242266 | 39.466012 |
# FL | Florida | -87.634938 | 24.523096 | -80.031362 | 31.000888 | 
# LA | Louisiana | -94.043147 | 28.928609 | -88.817017 | 33.019457 |

min_lon = -94.617919
min_lat = 24.523096 
max_lon = -75.242266
max_lat = 39.466012

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
    ds = ds.where(ds.coords['latitude']>min_lat,drop=True)
    ds = ds.where(ds.coords['latitude']<max_lat,drop=True)
    ds = ds.where(ds.coords['longitude']>min_lon,drop=True)
    ds = ds.where(ds.coords['longitude']<max_lon,drop=True)
    
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

myrows = range(1,3)

[getHadISDData(x) for x in myrows]

# Check to make sure data exported correctly
ds2 = xr.open_dataset('s3://ncai-humidity/had-isd/hourly/700001-26492.zarr', engine='zarr')

#ds.to_zarr(store= s3fs.S3Map(root=f's3://ncai-humidity/had-isd/hourly/'+stn_id+'.zarr', s3=s3 ,check=False))


#subset = ds.rio.clip_box(minx=min_lon, miny=min_lat, maxx=max_lon, maxy=max_lat)

#cropped_ds = ds.sel(ds.coords['latitude']=slice(min_lat,max_lat), ds.coords['longitude']=slice(min_lon,max_lon))

