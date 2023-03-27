
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

hadley = pd.read_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL_clean.parquet')

had0 = hadley[hadley.time.dt.year==2000]
had10 = hadley[hadley.time.dt.year==2010]
had21 = hadley[hadley.time.dt.year==2021]

hadley=hadley.reset_index()
had0=had0.reset_index()
had10=had10.reset_index()
had21=had21.reset_index()

# Get unique station locations for mapping and selecting SE stations

had_stn = hadley[('station_id','longitude','latitude')].unique()

had_locs = had_stn.filter(
        ~pl.all(pl.col('station_id').is_null())
    )

had_locs = had_locs.select(['latitude','longitude'])

# Match states back to long dataframe

hadley = hadley.join(had_locs, on=["latitude","longitude"])


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

# https://colab.research.google.com/drive/1B7gFBSr0eoZ5IbsA0lY8q3XL8n-3BOn4#scrollTo=Z9VEsSzGrrwE
# data2=data.sortby('time')

###  (5) One plot for that example station in 2010, where one line is the ISD dew point and one line is the HadISD dew point. That might make it very clear the difference in quality/outliers. Make sure there are labels axis, etc. 

isd = isd.rename(columns={"datetime": "time"})

matched_isd = hadley.merge(isd, left_on=("station_id","time"), right_on=("station_id","time"))

dsub = matched_isd[matched_isd["station_id"]=='720277-63843']

dsub=dsub[["date","dewpoints","Mean Dew Pt"]]
dsub = dsub.rename(columns={"dewpoints": "Hadley ISD (hourly)",
                           "Mean Dew Pt": "Original ISD (daily mean)" })

dsub["year"]=dsub.date.dt.year

dsub.plot("date", figsize=(15, 6))
plt.title("Comparison of dew point temperatures from Original ISD vs. Hadley ISD for 2006-2022 for station ID 720277")
plt.show()


d10 = had10
d10["date"] = d10['time'].dt.date

d10 = d10.groupby(["date","station_id"], as_index=False).mean(["temperatures","dewpoints"])

d10 = d10.reset_index()

isd = isd.rename(columns={"datetime": "date"})
isd["date"] = isd['date'].dt.date

dsub = d10[d10["station_id"]=='720277-63843']

matched_isd = dsub.merge(isd, left_on=("station_id","date"), right_on=("station_id","date"))

dsub=matched_isd[["date",
        "Mean Dew Pt","dewpoints"]]

dsub = dsub.rename(columns={"dewpoints": "Hadley ISD",
                           "Mean Dew Pt": "Original ISD " })

dsub.plot("date", figsize=(15, 6))
plt.title("Comparison of daily mean dew point temperatures from Original ISD vs. Hadley ISD in 2010 for station ID 720277-63843")
plt.show()
