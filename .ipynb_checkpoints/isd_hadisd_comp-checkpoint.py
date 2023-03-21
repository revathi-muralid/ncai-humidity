
# Created on: 3/20/23 by RM
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

# Read in HadISD data

hadley = pl.read_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL.parquet')

# Get unique station locations for mapping and selecting SE stations

had_stn = hadley.select(['station_id','longitude','latitude']).unique()

### (1) Map stations

with open('gz_2010_us_040_00_500k.json') as f:
    ncArea = json.load(f)

#initialize the map around northeast corner of alabama

ncMap = folium.Map(location=[32.318230,-86.902298], tiles='Stamen Toner', zoom_start=6)

#for each row in the dataset, plot the corresponding latitude and longitude on the map
for i,row in locs.iterrows():
    folium.CircleMarker((row.lat,row.long), radius=3, weight=2, color='red', fill_color='red', fill_opacity=.5).add_to(ncMap)

#add the boundaries of the US to the map
folium.GeoJson(ncArea).add_to(ncMap)

ncMap

# See if you can subset

#dsub = ds.isel(latitude=32.217)

d4_sub = ds_masked.where(ds_masked.station_id=='744658-53889',drop=True)

d4_sub = data.where(data.station_id=='744658-53889',drop=True)

d4_masked = d4_sub.where(d4_sub['temperatures'] != -2e+30)
d4_masked = d4_masked.where(d4_masked['dewpoints'] != -2e+30)
d4_masked = d4_masked.where(d4_masked['windspeeds'] != -2e+30)
d4_masked = d4_masked.where(d4_masked['stnlp'] != -2e+30)
d4_masked.temperatures.plot()
d4_masked.dewpoints.plot()
d4_masked.windspeeds.plot()
d4_masked.stnlp.plot()
plt.title("Time series for station ID 747808-63803")
plt.xlabel("Time of Measurement (Year)")
plt.ylabel("Temperature (deg. C)")

d4_sub.isel(1000).plot.line(color="purple", marker="o")
dat2d = data.isel(time=1000)
dat2d.temperatures.plot()

# https://colab.research.google.com/drive/1B7gFBSr0eoZ5IbsA0lY8q3XL8n-3BOn4#scrollTo=Z9VEsSzGrrwE
# data2=data.sortby('time')
