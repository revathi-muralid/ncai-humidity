# Created on: 12/20/22 by RM
# Last updated: 1/12/22 by RM
# Purpose: To visualize and explore NOAA Integrated Surface Database (ISD) in situ humidity data

# ISD consists of global hourly observations compiled from an array of different sources
# The data are stored in the S3 bucket s3://noaa-isd-pds in fixed with text file format

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
    path='s3://ncai-humidity/isd/daily/NC-2000-DAILY.parquet'
)

dat10=dat10.rename(columns={"('Control', 'datetime')":"datetime","('Control', 'USAF')":"USAF_ID",
"('Control', 'WBAN')":"WBAN_ID","('Control', 'latitude')":"lat","('Control', 'longitude')":"long","('Control', 'elevation')":"elev"})

for col in dat10.columns:
    print(col)

d10_locs = dat10[["USAF_ID","WBAN_ID","lat","long"]]

d10_locs=d10_locs.drop_duplicates(["USAF_ID","WBAN_ID"])

# 82 unique ID combos/stations

### (1) Map stations

with open('NC-37-north-carolina-counties.json') as f:
    ncArea = json.load(f)

#initialize the map around NC centroid

ncMap = folium.Map(location=[35.782169,-80.793457], tiles='Stamen Toner', zoom_start=6)

#for each row in the dataset, plot the corresponding latitude and longitude on the map
for i,row in d10_locs.iterrows():
    folium.CircleMarker((row.lat,row.long), radius=3, weight=2, color='red', fill_color='red', fill_opacity=.5).add_to(ncMap)

#add the boundaries of NC Counties to the map
folium.GeoJson(ncArea).add_to(ncMap)

ncMap

### (2) Summarize QC filtered data

summ = dat10.describe()

dat10.count()

# Time series for an individual station

d10 = dat10[dat10["USAF_ID"]=='720277']

d10_temp=d10[["USAF_ID","WBAN_ID","date",
        "Mean Air Temp.",
        "Mean Dew Pt",
        "Mean RH_TAVE",
        "Mean Min RH T",
        "Mean Max RH T"]]

d10_temp.plot("date", figsize=(15, 6))
plt.show()

isd.plot("('Control', 'datetime')", figsize=(15, 6))
plt.show()


# Summarize completeness of data

msno.matrix(dat10[["Mean Air Temp.",
        "Mean Dew Pt",
        "Mean RH_TAVE",
        "Mean Min RH T",
        "Mean Max RH T"]])
plt.show()

grouped = dat10.groupby("USAF_ID")
sizes = grouped.size().values * len(dat10.columns)

num_of_nans = sizes - grouped.count().sum(axis=1)
out = num_of_nans / sizes
out2 = out.to_frame().rename(columns={0: 'NaPercent'})

dat10.plot("date", ["Mean Air Temp.","Mean Dew Pt"], figsize=(15, 6))
plt.show()






#### SCRATCH
        
# Read in and process GHCN data
ghcn_data = (
    pl.from_arrow(
        ds.dataset(queries)
        .scanner(
            columns = ['ID','DATE', 'ELEMENT', 'DATA_VALUE'],
            filter = ds.field('ELEMENT').isin(elements)
        )
        .to_table()
    )
    .with_column(
        pl.col('DATE').str.strptime(pl.Date, "%Y%m%d")
    )
    .filter(pl.col('DATE') >= pl.datetime(2020,1,1))
    .groupby(['DATE', 'ELEMENT'])
    .agg(
        pl.col('DATA_VALUE').mean()
    )
    .pivot(
        values = 'DATA_VALUE', 
        index = 'DATE', 
        columns = 'ELEMENT'
    )    
)



