# Created on: 1/23/23 by RM
# Last updated: 1/23/23 by RM
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
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import json
import folium
from folium import plugins
import missingno as msno
import seaborn as sns
import xarray

# Build time series figures

# Scan in station data
queries = []
study_states = list(("AL","AR","FL","GA","KY","LA","MS","NC","SC","TN","VA"))

for state in study_states:
    q = (
        ds.dataset("s3://ncai-humidity/isd/daily/"+str(state)+"-ALLYEARS-DAILY-STATE.parquet", partitioning='hive')
    )
    queries.append(q)

### Read in example dataset to get colnames    

dat10 = wr.s3.read_parquet(
    path='s3://ncai-humidity/isd/daily/AL-ALLYEARS-DAILY.parquet'
)

for col in dat10.columns:
    print(col)

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

dfp = df.to_pandas()

### (0) Make dataframe of unique locations

locs=dfp.drop_duplicates(subset=["USAF_ID","WBAN_ID"])

locs = locs[["USAF_ID","WBAN_ID","lat","long"]]

locs = locs[locs["lat"].notnull()]

# 82 unique ID combos/stations

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

### (2) Summarize QC filtered data

dfp2 = dfp[["state","Mean Air Temp.","Mean Dew Pt","Mean Sea Press.","Mean Atm Press.","Mean Atm Press.","Mean Temp Diff","Calculated RH","MetPy RH"]]

summ = dfp2.groupby('state').describe()

summ.to_csv('s3://ncai-humidity/isd/state-summ-stats.csv',index=True)

dfp.count()

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

### (3) Create state-level time series plots

viridis = mpl.colormaps['tab20'].resampled(11)

statePalette = {'AL': viridis.colors[1],
                'AR': viridis.colors[2],
                'FL': viridis.colors[3],
                'GA': viridis.colors[4],
                'KY': viridis.colors[5],
                'LA': viridis.colors[6],
                'MS': viridis.colors[7],
                'NC': viridis.colors[8],
                'SC': viridis.colors[9],
                'TN': viridis.colors[10],
                'VA': viridis.colors[0]}

fig, axes = plt.subplots(3,4, figsize=(12,5))
for (state, group), ax in zip(dfp.groupby('state'), axes.flatten()):
    mycolor=statePalette[state]
    group.plot(x='datetime', y='Mean Air Temp.', kind='line', ax=ax, title=state,
              color=mycolor,legend=False)
    ax.set_xlabel('')
    fig.tight_layout()

fig.suptitle('Time Series of Mean Daily Air Temperatures in 11 SE States')
fig.subplots_adjust(top=0.88)
fig.delaxes(axes[2][3])

# Summarize completeness of data

msno.matrix(dfp[["Mean Air Temp.",
        "Mean Dew Pt",
        "Calculated RH",
        "MetPy RH"]])
plt.show()

grouped = dat10.groupby("USAF_ID")
sizes = grouped.size().values * len(dat10.columns)

num_of_nans = sizes - grouped.count().sum(axis=1)
out = num_of_nans / sizes
out2 = out.to_frame().rename(columns={0: 'NaPercent'})







#### ADD STATE COLUMN

dat = wr.s3.read_parquet(
    path='s3://ncai-humidity/isd/daily/VA-ALLYEARS-DAILY.parquet'
)

dat["state"] = "VA"

dat.to_parquet('s3://ncai-humidity/isd/daily/VA-ALLYEARS-DAILY-STATE.parquet',index=False)
