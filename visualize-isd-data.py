# Created on: 12/20/22 by RM
# Last updated: 12/21/22 by RM
# Purpose: To visualize and explore NOAA Integrated Surface Database (ISD) in situ humidity data

# ISD consists of global hourly observations compiled from an array of different sources
# The data are stored in the S3 bucket s3://noaa-isd-pds in fixed with text file format

### FIRST: Run 'pip install folium'
# pip install s3fs

### THEN: go through the rest!

# Import libraries
import awswrangler as wr
import polars as pl
import pyarrow.dataset as ds
import boto3
import us
import s3fs
from functools import partial
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import json
import folium
from folium import plugins

# Build time series figures

dat10 = wr.s3.read_parquet(
    path='s3://ncai-humidity/isd/NC-2010.parquet'
)

dat10=dat10.rename(columns={"('Control', 'USAF')":"USAF_ID",
"('Control', 'WBAN')":"WBAN_ID","('Control', 'latitude')":"lat","('Control', 'longitude')":"long"})

d10_locs = dat10[["USAF_ID","WBAN_ID","lat","long"]]

d10_locs=d10_locs.drop_duplicates()

# 82 unique ID combos/stations

# Map stations

with open('NC-37-north-carolina-counties.json') as f:
    ncArea = json.load(f)

#initialize the map around NC centroid

ncMap = folium.Map(location=[35.782169,-80.793457], tiles='Stamen Toner', zoom_start=6)

#for each row in the Starbucks dataset, plot the corresponding latitude and longitude on the map
for i,row in d10_locs.iterrows():
    folium.CircleMarker((row.lat,row.long), radius=3, weight=2, color='red', fill_color='red', fill_opacity=.5).add_to(ncMap)

#add the boundaries of NC Counties to the map
folium.GeoJson(ncArea).add_to(ncMap)

ncMap

# Time series for an individual station

d10 = dat10[dat10["USAF_ID"]=='720277']

for col in dat10.columns:
    print(col)

d10_temp=d10[["USAF_ID","WBAN_ID","('Control', 'datetime')",
        "('air temperature', 'temperature')",
        "('dew point', 'temperature')",
        "('Relative-Humidity-Temperature 1', 'T_AVE')",
        "('Hourly-RH-Temperature 1', 'MIN_RH_T')",
        "('Hourly-RH-Temperature 1', 'MAX_RH_T')"]]

d10_temp.plot("('Control', 'datetime')", figsize=(15, 6))
plt.show()