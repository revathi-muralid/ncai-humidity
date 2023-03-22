
# Created on: 3/20/23 by RM
# Last updated: 3/21/23 by RM

# Import libraries
import polars as pl
import awswrangler as wr
import folium
from folium import plugins
from folium.features import DivIcon
import reverse_geocoder as rgcr
import matplotlib as mpl
import matplotlib.pyplot as plt
import missingno as msno
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

import boto3
import us
import s3fs
from functools import partial
import datetime
from datetime import date
import json

import seaborn as sns
import xarray as xr
import zarr


######################  READ IN HADLEY ISD DATA ######################  

hadley = pl.scan_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL.parquet').collect()

# Get unique station locations for mapping and selecting SE stations

had_stn = hadley.select(['station_id','longitude','latitude']).unique()

had_locs = had_stn.filter(
        ~pl.all(pl.col('station_id').is_null())
    )

had_locs = had_locs.select(['latitude','longitude'])

# Get state name using reverse geocoder

state=[]
for i in range(len(had_locs)):
    results = rgcr.search((had_locs['latitude'][i],had_locs['longitude'][i]))
    state.append(results[0]['admin1'])

had_locs = had_locs.with_columns(
pl.Series(name="state",values=state)
)

# Match states back to long dataframe

hadley = hadley.join(had_locs, on=["latitude","longitude"])

# Remove Maryland, Delaware, Indiana, Missouri, West Virginia, Illinois, Ohio, Texas records

excludes = ["Maryland", "Delaware", "Indiana", "Missouri", "West Virginia", "Illinois", "Ohio", "Texas"]

hadley = hadley.filter(
~pl.col('state').is_in(excludes) )

###################### (1) - Summarize completeness of data ######################

# hadclean = hadley.with_columns([
# pl.col("dewpoints").fill_null(np.nan),
#     pl.col("slp").fill_null(np.nan),
#     pl.col("stnlp").fill_null(np.nan),
#     pl.col("temperatures").fill_null(np.nan),
#     pl.col("windspeeds").fill_null(np.nan),
# ])
# hadclean.null_count()

had_msno = pd.read_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL.parquet')

#################### (2) Create state-level time series plots ####################

# Get state name using reverse geocoder

hadms_locs = had_msno[["station_id","latitude","longitude"]]
hadms_locs = hadms_locs.drop_duplicates()
hadms_locs = hadms_locs.dropna()
hadms_locs = hadms_locs.reset_index()

state=[]
for i in range(len(hadms_locs)):
    results = rgcr.search((hadms_locs['latitude'][i],hadms_locs['longitude'][i]))
    state.append(results[0]['admin1'])

hadms_locs['state'] = state

had_msno = had_msno.merge(hadms_locs[["station_id","state"]])

had_msno = had_msno[~had_msno['state'].isin(excludes)]

viridis = mpl.colormaps['tab20'].resampled(11)

statePalette = {'Alabama': viridis.colors[1],
                'Arkansas': viridis.colors[2],
                'Florida': viridis.colors[3],
                'Georgia': viridis.colors[4],
                'Kentucky': viridis.colors[5],
                'Louisiana': viridis.colors[6],
                'Mississippi': viridis.colors[7],
                'North Carolina': viridis.colors[8],
                'South Carolina': viridis.colors[9],
                'Tennessee': viridis.colors[10],
                'Virginia': viridis.colors[0]}

fig, axes = plt.subplots(3,4, figsize=(12,5))
for (state, group), ax in zip(had_msno.groupby('state'), axes.flatten()):
    mycolor=statePalette[state]
    group.plot(x='time', y='temperatures', kind='line', ax=ax, title=state,
              color=mycolor,legend=False)
    ax.set_xlabel('')
    fig.tight_layout()

fig.suptitle('Hadley Time Series of Hourly Air Temperatures in 11 SE States')
fig.subplots_adjust(top=0.88)
fig.delaxes(axes[2][3])

########################### (3) Map stations #######################################

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

######################### Read in original ISD data #########################

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
isd["station_id"] = isd["USAF_ID"]+"-"+isd["WBAN_ID"]

################ Match original ISD stations to Hadley ISD stations ################

isd_stn = pd.DataFrame(isd.station_id.unique())
isd_stn = isd_stn.rename(columns={0: "station_id"})

had_stn = had_stn.to_pandas()
matched_isd = had_stn.merge(isd_stn, left_on="station_id", right_on="station_id")

# See if you can subset

d4_sub = ds_masked.where(ds_masked.station_id=='744658-53889',drop=True)

d4_sub = data.where(data.station_id=='744658-53889',drop=True)

d4_masked.temperatures.plot()
d4_masked.dewpoints.plot()
d4_masked.windspeeds.plot()
d4_masked.stnlp.plot()
plt.title("Time series for station ID 747808-63803")
plt.xlabel("Time of Measurement (Year)")
plt.ylabel("Temperature (deg. C)")

# https://colab.research.google.com/drive/1B7gFBSr0eoZ5IbsA0lY8q3XL8n-3BOn4#scrollTo=Z9VEsSzGrrwE
# data2=data.sortby('time')
