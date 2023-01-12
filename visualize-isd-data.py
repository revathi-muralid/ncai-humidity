# Created on: 12/20/22 by RM
# Last updated: 1/11/22 by RM
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

isd.plot("('Control', 'datetime')", figsize=(15, 6))
plt.show()

# Summarize QC filtered data
        
dat0 = isd_data.filter(pl.col("('Control', 'datetime')").is_between(datetime(2000, 1, 1), datetime(2001, 1, 1)))

dat10 = isd_data.filter(pl.col("('Control', 'datetime')").is_between(datetime(2010, 1, 1), datetime(2011, 1, 1)))

dat21 = isd_data.filter(pl.col("('Control', 'datetime')").is_between(datetime(2021, 1, 1), datetime(2022, 1, 1)))

summ = dat0.describe()

for col in df.columns:
    print(col)

dat10.count()

summ[["describe", "('dew point', 'temperature')",
"('air temperature', 'temperature')",
"Mean Air Temp.",
"Mean Dew Pt Temp.",
"Mean Dew Pt vs. Air Temp"]]

summ = dat10.describe()

for col in dat0a.columns:
    print(col)

# Summarize completeness of data

### FIX CODE
msno.matrix(dat0[["('air temperature', 'temperature')","('dew point', 'temperature')"]])
plt.show()

grouped = dat10.groupby("('Control', 'USAF')")
sizes = grouped.size().values * len(dat10.columns)

num_of_nans = sizes - grouped.count().sum(axis=1)
out = num_of_nans / sizes
out2 = out.to_frame().rename(columns={0: 'NaPercent'})

dat0a = dat21.to_pandas()
dat0a.plot("('Control', 'datetime')", "('air temperature', 'temperature')", figsize=(15, 6))
plt.show()

dat0a.plot("('Control', 'datetime')", "('dew point', 'temperature')", figsize=(15, 6))
plt.show()






#### SCRATCH

px.line(isd_data.to_pandas(),               # covert to Pandas DataFrame
        x = "('Control', 'datetime')", 
        y = "('dew point', 'temperature')"
           )


d10 = isd_data[isd_data["('Control', 'USAF')"]=="746929"]



# Summarize completeness of data

isd=pd.DataFrame(isd_data)

isd=isd_data.rename(columns={"('Control', 'USAF')":"USAF_ID",
"('Control', 'WBAN')":"WBAN_ID","('Control', 'latitude')":"lat","('Control', 'longitude')":"long"})
d10 = isd_data[isd_data["USAF_ID"]=='720277']

for col in dat10.columns:
    print(col)

d10_temp=d10[["USAF_ID","WBAN_ID","('Control', 'datetime')",
        "('air temperature', 'temperature')",
        "('dew point', 'temperature')",
        "('Relative-Humidity-Temperature 1', 'T_AVE')",
        "('Hourly-RH-Temperature 1', 'MIN_RH_T')",
        "('Hourly-RH-Temperature 1', 'MAX_RH_T')"]]

# Elements of interest
elements = ['TMAX', 'PRCP', 'TMIN', 'RHAV', 'TAVG']

        
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

dat0 = wr.s3.read_parquet(
    path='s3://ncai-humidity/isd/NC-2000.parquet')

dat10 = wr.s3.read_parquet(
    path='s3://ncai-humidity/isd/NC-2010.parquet'
)
    
dat21 = wr.s3.read_parquet(
    path='s3://ncai-humidity/isd/NC-2021.parquet'
)

# Variables of interest for summarizing - 2010 and beyond

summ = dat10.describe()

for col in dat10.columns:
    print(col)

dat10.count()

summ[["('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('air temperature', 'temperature')","('dew point', 'temperature')","('sea level pressure', 'pressure')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Relative-Humidity-Raw 1', 'RH_PERIOD')","('Relative-Humidity-Raw 2', 'RH_PERIOD')","('Relative-Humidity-Raw 3', 'RH_PERIOD')","('Relative-Humidity-Temperature 1', 'RH_T_PERIOD')","('Relative-Humidity-Temperature 1', 'T_AVE')","('Relative-Humidity-Temperature 1', 'RH_AVE')","('Hourly-RH-Temperature 1', 'MIN_RH_T')","('Hourly-RH-Temperature 1', 'MAX_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH')"]]


summ[["('wind', 'speed QC')","('air temperature', 'temperature QC')","('sea level pressure', 'pressure QC')",
  "('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')","('Relative-Humidity-Raw 1', 'RH_QC')",
  "('Relative-Humidity-Raw 2', 'RH_QC')","('Relative-Humidity-Raw 3', 'RH_QC')",
  "('Relative-Humidity-Temperature 1', 'T_AVE_QC')","('Relative-Humidity-Temperature 1', 'T_AVE_QC_FLG')",
  "('Relative-Humidity-Temperature 1', 'RH_AVE_QC')","('Relative-Humidity-Temperature 1', 'RH_AVE_QC_FLG')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC_FLG')", "('Hourly-RH-Temperature 1', 'MAX_RH_T_QC')","('Hourly-RH-Temperature 1', 'MAX_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_QC')","('Hourly-RH-Temperature 1', 'SD_RH_QC_FLG')"]]














station_info = wr.s3.read_fwf(
    path='s3://ncai-humidity/ghcnd-stations.txt'

nc_stations = (
    pl.from_pandas(station_info)
    .filter(pl.col('STATE') == 'NC')
    .filter(pl.col('NAME').str.contains(r"ASHEVILLE"))
    .get_column('ID')
)

# Load station files for NC Stations
station_files = [f"./ghcn-data/parquet/by_station/STATION={station}/" for station in nc_stations]


# Scan in station data
queries = []
for state in state_names:
    q = (
        ds.dataset(station, partitioning='hive')
    )
    queries.append(q)
   
# Elements of interest
elements = ['TMAX', 'PRCP', 'TMIN', 'RHAV', 'TAVG']

        
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







convert.convert('ncai-humidity',2000,2001,country=['US'])


# Make parser that converts to parquet at end
# toughest part is getting file name you wanna save as in parser function
# converter.py in ISD subpackage




# Read in and process default and added variables
ghcn_data = (
    pl.from_arrow(
        reader.read( year=[2000], country=['US'], state=['NC'], parser=parser )
        .loc([('Control', 'USAF'),('Control', 'WBAN'),('Control', 'latitude'),('Control', 'longitude'),('Control', 'elevation'),
                      ('Control', 'QC process'),('Control', 'datetime'),('wind', 'speed'),('wind', 'speed QC'),('air temperature', 'temperature'),
                       ('air temperature', 'temperature QC'),('dew point', 'temperature'),('dew point', 'temperature QC'),
                       ('sea level pressure', 'pressure'),('sea level pressure', 'pressure QC')]
        )
        .to_table()
    )
)



# Load state abbreviations for USA
# state_names = [state.abbr for state in us.states.STATES]
