# Created on: 12/9/22 by RM
# Last updated: 1/11/23 by RM
# Purpose: To summarize and explore NOAA Integrated Surface Database (ISD) in situ humidity data

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
import missingno as msno
import folium
from folium import plugins
import plotly.express as px

myyr = 2007

# Load years of interest
study_years = list(range(myyr,myyr+1))

### 2008-2022
myvars = ["('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('wind', 'speed QC')","('air temperature', 'temperature')","('air temperature', 'temperature QC')","('dew point', 'temperature')","('dew point', 'temperature QC')","('sea level pressure', 'pressure')","('sea level pressure', 'pressure QC')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')","('Relative-Humidity-Temperature 1', 'RH_AVE')","('Relative-Humidity-Temperature 1', 'RH_AVE_QC')","('Relative-Humidity-Raw 1', 'RH_PERIOD')","('Relative-Humidity-Raw 2', 'RH_PERIOD')","('Relative-Humidity-Raw 3', 'RH_PERIOD')","('Relative-Humidity-Raw 1', 'RH_QC')","('Relative-Humidity-Raw 2', 'RH_QC')","('Relative-Humidity-Raw 3', 'RH_QC')","('Relative-Humidity-Temperature 1', 'RH_T_PERIOD')","('Relative-Humidity-Temperature 1', 'T_AVE')","('Hourly-RH-Temperature 1', 'MIN_RH_T')","('Hourly-RH-Temperature 1', 'MAX_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH')","('Relative-Humidity-Temperature 1', 'T_AVE_QC')","('Relative-Humidity-Temperature 1', 'T_AVE_QC_FLG')","('Relative-Humidity-Temperature 1', 'RH_AVE_QC_FLG')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'MAX_RH_T_QC')","('Hourly-RH-Temperature 1', 'MAX_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_QC')","('Hourly-RH-Temperature 1', 'SD_RH_QC_FLG')"]

### 2006-7
subvars = ["('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('wind', 'speed QC')","('air temperature', 'temperature')","('air temperature', 'temperature QC')","('dew point', 'temperature')","('dew point', 'temperature QC')","('sea level pressure', 'pressure')","('sea level pressure', 'pressure QC')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')","('Relative-Humidity-Raw 1', 'RH_PERIOD')","('Relative-Humidity-Raw 2', 'RH_PERIOD')","('Relative-Humidity-Raw 3', 'RH_PERIOD')","('Relative-Humidity-Raw 1', 'RH_QC')","('Relative-Humidity-Raw 2', 'RH_QC')","('Relative-Humidity-Raw 3', 'RH_QC')"]

### 2001-2005
subvars2 = ["('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('wind', 'speed QC')","('air temperature', 'temperature')","('air temperature', 'temperature QC')","('dew point', 'temperature')","('dew point', 'temperature QC')","('sea level pressure', 'pressure')","('sea level pressure', 'pressure QC')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')","('Relative-Humidity-Temperature 1', 'RH_AVE')","('Relative-Humidity-Temperature 1', 'RH_AVE_QC')","('Relative-Humidity-Temperature 1', 'RH_T_PERIOD')","('Relative-Humidity-Temperature 1', 'T_AVE')","('Hourly-RH-Temperature 1', 'MIN_RH_T')","('Hourly-RH-Temperature 1', 'MAX_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH')","('Relative-Humidity-Temperature 1', 'T_AVE_QC')","('Relative-Humidity-Temperature 1', 'T_AVE_QC_FLG')","('Relative-Humidity-Temperature 1', 'RH_AVE_QC_FLG')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'MAX_RH_T_QC')","('Hourly-RH-Temperature 1', 'MAX_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_QC')","('Hourly-RH-Temperature 1', 'SD_RH_QC_FLG')"]

### 2000
subvars2000 = ["('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('wind', 'speed QC')","('air temperature', 'temperature')","('air temperature', 'temperature QC')","('dew point', 'temperature')","('dew point', 'temperature QC')","('sea level pressure', 'pressure')","('sea level pressure', 'pressure QC')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')"]


# Scan in station data
queries = []
for year in study_years:
    q = (
        ds.dataset("s3://ncai-humidity/isd/raw/NC-"+str(year)+".parquet", partitioning='hive')
    )
    queries.append(q)
    
# QC flags for inclusion
include_qc = ["5","1","9","A","4","0"]

df = (
    pl.from_arrow(
        ds.dataset(queries)
        .scanner(
            columns = subvars#,
            )
        .to_table()
     )
    .filter(pl.col("('Control', 'USAF')")!="999999")
    .with_column(pl.col("('Control', 'datetime')").dt.strftime("%Y-%m-%d").alias("date"))
    #.with_columns(pl.col("('Control', 'datetime')").str.strptime(pl.Datetime, "%Y-%m-%d %H:%m:%s.%f"))
    .sort(pl.col("('Control', 'datetime')"))   
)

df_forjoin = df.select(["date","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')"])

df_forjoin = df_forjoin.unique(subset=["date", "('Control', 'USAF')"])

### INCLUDE FOR ALL YEARS

isd_airtemp = (
    df
    .filter(pl.col("('air temperature', 'temperature QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('air temperature', 'temperature')"]).mean().alias('Mean Air Temp.')
    ])
)

isd_dewpt = (
    df
    .filter(pl.col("('dew point', 'temperature QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('dew point', 'temperature')"]).mean().alias('Mean Dew Pt')
    ])
)

isd_press = (
    df
    .filter(pl.col("('sea level pressure', 'pressure QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('sea level pressure', 'pressure')"]).mean().alias('Mean Sea Press.')
    ])
)

isd_atm = (
    df
    .filter(pl.col("('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')"]).mean().alias('Mean Atm Press.')
    ])
)
###################

### SKIP FOR 2000, 2006-7

isd_rhave = (
    df
    .filter(pl.col("('Relative-Humidity-Temperature 1', 'RH_AVE_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Relative-Humidity-Temperature 1', 'RH_AVE')"]).mean().alias('Mean RH_AVE')
    ])
)

isd_rh_tave = (
    df
    .filter(pl.col("('Relative-Humidity-Temperature 1', 'T_AVE_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Relative-Humidity-Temperature 1', 'T_AVE')"]).mean().alias('Mean RH_TAVE')
    ])
)

isd_minrh_t = (
    df
    .filter(pl.col("('Hourly-RH-Temperature 1', 'MIN_RH_T_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Hourly-RH-Temperature 1', 'MIN_RH_T')"]).mean().alias('Mean Min RH T')
    ])
)

isd_maxrh_t = (
    df
    .filter(pl.col("('Hourly-RH-Temperature 1', 'MAX_RH_T_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Hourly-RH-Temperature 1', 'MAX_RH_T')"]).mean().alias('Mean Max RH T')
    ])
)

isd_sdrh_t = (
    df
    .filter(pl.col("('Hourly-RH-Temperature 1', 'SD_RH_T_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Hourly-RH-Temperature 1', 'SD_RH_T')"]).mean().alias('Mean SD RH T')
    ])
)

isd_sdrh = (
    df
    .filter(pl.col("('Hourly-RH-Temperature 1', 'SD_RH_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Hourly-RH-Temperature 1', 'SD_RH')"]).mean().alias('Mean SD RH')
    ])
)

#############

### SKIP FOR 2000-2005

isd_rhraw1 = (
    df
    .filter(pl.col("('Relative-Humidity-Raw 1', 'RH_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Relative-Humidity-Raw 1', 'RH_PERIOD')"]).mean().alias('Mean Raw RH 1')
    ])
)

isd_rhraw2 = (
    df
    .filter(pl.col("('Relative-Humidity-Raw 2', 'RH_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Relative-Humidity-Raw 2', 'RH_PERIOD')"]).mean().alias('Mean Raw RH 2')
    ])
)

isd_rhraw3 = (
    df
    .filter(pl.col("('Relative-Humidity-Raw 3', 'RH_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')"])
    .agg([
        pl.col(["('Relative-Humidity-Raw 3', 'RH_PERIOD')"]).mean().alias('Mean Raw RH 3')
    ])
)

###############

# dfs = [df, isd_airtemp,
#     isd_dewpt,
#     isd_press,
#     isd_atm,
#     isd_rhave,
#     isd_rh_tave,
#     isd_rhraw1, isd_rhraw2, isd_rhraw3,
#     isd_minrh_t, isd_maxrh_t, isd_sdrh_t, isd_sdrh
# ]

### 2008-2022
isd_data = df_forjoin.join(isd_airtemp.join(isd_dewpt.join(isd_press.join(isd_atm.join(isd_rhave.join(isd_rh_tave.join(isd_rhraw1.join(isd_rhraw2.join(isd_rhraw3.join(isd_minrh_t.join(isd_maxrh_t.join(isd_sdrh_t.join(isd_sdrh,on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),                                                                                                                       on=["date", "('Control', 'USAF')"],
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'), 
on=["date", "('Control', 'USAF')"],
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'), 
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'
)

### 2001-2005
isd_data = df_forjoin.join(isd_airtemp.join(isd_dewpt.join(isd_press.join(isd_atm.join(isd_rhave.join(isd_rh_tave.join(isd_minrh_t.join(isd_maxrh_t.join(isd_sdrh_t.join(isd_sdrh,on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'), 
on=["date", "('Control', 'USAF')"],
    how='left'), 
on=["date", "('Control', 'USAF')"],
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'), 
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'
)

### 2006-7
isd_data = df_forjoin.join(isd_airtemp.join(isd_dewpt.join(isd_press.join(isd_atm.join(isd_rhraw1.join(isd_rhraw2.join(isd_rhraw3,     on=["date", "('Control', 'USAF')"], 
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'), 
on=["date", "('Control', 'USAF')"],
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'),  
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'
)

### 2000
isd_data = df_forjoin.join(isd_airtemp.join(isd_dewpt.join(isd_press.join(isd_atm, on=["date", "('Control', 'USAF')"],
    how='left'), 
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left'),
on=["date", "('Control', 'USAF')"],
    how='left')

isd_data = isd_data.with_columns([
    (pl.col("Mean Air Temp.") - pl.col("Mean Dew Pt")).alias('Mean Temp Diff')
])

isd = isd_data.to_pandas()

isd.to_parquet('s3://ncai-humidity/isd/daily/NC-'+str(myyr)+'-DAILY.parquet',index=False)

### END


