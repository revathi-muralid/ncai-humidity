
# Created on: 3/20/23 by RM
# Last updated: 3/20/23 by RM

# Import libraries
import polars as pl
import awswrangler as wr
import folium
from folium import plugins
from folium.features import DivIcon

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

import missingno as msno
import seaborn as sns
import xarray as xr
import numpy as np
import zarr

# Read in HadISD data

hadley = pl.read_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL.parquet')

# Get unique station locations for mapping and selecting SE stations

had_stn = hadley.select(['station_id','longitude','latitude']).unique()

had_locs = had_stn.filter(
        ~pl.all(pl.col('station_id').is_null())
    )

had_locs = had_locs.select(['latitude','longitude'])


### (1) Map stations

with open('gz_2010_us_040_00_500k.json') as f:
    ncArea = json.load(f)

#initialize the map around northeast corner of alabama

ncMap = folium.Map(location=[32.318230,-86.902298], tiles='Stamen Toner', zoom_start=6)

#for each row in the dataset, plot the corresponding latitude and longitude on the map
for i,row in had_locs.iterrows():
    folium.CircleMarker((row['latitude'],row['longitude']), radius=3, weight=2, color='red', fill_color='red', fill_opacity=.5).add_to(ncMap)
    folium.map.Marker((row['latitude'],row['longitude']),
                      icon=DivIcon(
                          icon_size=(10,10),
                          icon_anchor=(5,14),
                          html=f'<div style="font-size: 14pt">%s</div>' % str(i),
                      )
                     ).add_to(ncMap)

#add the boundaries of the US to the map
folium.GeoJson(ncArea).add_to(ncMap)

ncMap

### Read in original ISD data

# Scan in station data
queries = []
study_states = list(("AL","AR","FL","GA","KY","LA","MS","NC","SC","TN","VA"))

for state in study_states:
    q = (
        ds.dataset("s3://ncai-humidity/isd/daily/"+str(state)+"-ALLYEARS-DAILY-STATE.parquet", partitioning='hive')
    )
    queries.append(q)
    
### Read in all years and states into one df
    
myvars = ["('Control', 'datetime')","('Control', 'USAF')",
          "('Control', 'WBAN')","('Control', 'latitude')",
          "('Control', 'longitude')","('Control', 'elevation')",
          "Mean Air Temp.","Mean Dew Pt","Mean Sea Press.",
          "Mean Atm Press.","Mean Temp Diff",
          "Calculated RH","MetPy RH","state"]
    
df = (
    pl.from_arrow(
        ds.dataset(queries)
        .scanner(
            columns = myvars#,
            )
        .to_table()
     )
)    

df=df.rename({"('Control', 'datetime')":"datetime","('Control', 'USAF')":"USAF_ID",
"('Control', 'WBAN')":"WBAN_ID","('Control', 'latitude')":"lat","('Control', 'longitude')":"long","('Control', 'elevation')":"elev"})

isd = df.to_pandas()

# Match original ISD stations to Hadley ISD stations

import math
mat = []
for i,j in zip(had_locs['latitude'],had_locs['longitude']):
    k = []
    for l,m in zip(isd['lat'],isd['lon']):
        k.append(math.hypot(i - l, j - m))
    mat.append(k)

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
