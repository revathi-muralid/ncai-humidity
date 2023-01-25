# Created on: 1/18/22 by RM
# Last updated: 1/23/23 by RM
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
import pandas as pd
import metpy
import math
from metpy.calc import relative_humidity_from_dewpoint
from metpy.units import units
import numpy.ma as ma

myyr = 2000
mystate= 'VA'

# DONE: AL, AR, FL, GA, KY, LA, MS, NC, SC, TN, 
# TO DO: VA

# Load years of interest
study_years = list(range(myyr,myyr+1))

### 2000
subvars2000 = ["('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('wind', 'speed QC')","('air temperature', 'temperature')","('air temperature', 'temperature QC')","('dew point', 'temperature')","('dew point', 'temperature QC')","('sea level pressure', 'pressure')","('sea level pressure', 'pressure QC')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')"]


# Scan in station data
queries = []
study_years = list(range(2000,2023))
for year in study_years:
    q = (
        ds.dataset("s3://ncai-humidity/isd/raw/"+str(mystate)+"-"+str(year)+".parquet", partitioning='hive')
    )
    queries.append(q)
    
# QC flags for inclusion
include_qc = ["5","1","9","A","4","0"]

df = (
    pl.from_arrow(
        ds.dataset(queries)
        .scanner(
            columns = subvars2000#,
            )
        .to_table()
     )
    .filter((pl.col("('Control', 'USAF')")!="999999") & (pl.col("('Control', 'WBAN')")!="999999"))
    .with_column(pl.col("('Control', 'datetime')").dt.strftime("%Y-%m-%d").alias("date"))
    .sort(pl.col("('Control', 'datetime')"))   
)

df2 = df.to_pandas()
df2.count()

### QC FILTERING COUNTS FOR 2010
# Filtering out records with both IDs == 999999 results in:
# 1757917 --> 1441510 records (~300k records dropped)
# 'Relative-Humidity-Raw' variables: 3225 --> 3225
# 'Relative-Humidity-Temperature' variables: 315276 --> 0
# 'Hourly-RH-Temperature' variables: 26273 --> 0
###

df_forjoin = df.select(["date","('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')"])

df_forjoin = df_forjoin.unique(subset=["date", "('Control', 'USAF')","('Control', 'WBAN')"])

### INCLUDE FOR ALL YEARS

isd_airtemp = (
    df
    .filter(pl.col("('air temperature', 'temperature QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    .agg([
        pl.col(["('air temperature', 'temperature')"]).mean().alias('Mean Air Temp.')
    ])
)

isd_dewpt = (
    df
    .filter(pl.col("('dew point', 'temperature QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    .agg([
        pl.col(["('dew point', 'temperature')"]).mean().alias('Mean Dew Pt')
    ])
)

isd_press = (
    df
    .filter(pl.col("('sea level pressure', 'pressure QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    .agg([
        pl.col(["('sea level pressure', 'pressure')"]).mean().alias('Mean Sea Press.')
    ])
)

isd_atm = (
    df
    .filter(pl.col("('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    .agg([
        pl.col(["('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')"]).mean().alias('Mean Atm Press.')
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

### 2000
isd_data = df_forjoin.join(isd_airtemp.join(isd_dewpt.join(isd_press.join(isd_atm, on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left'), 
on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left'),
on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left'),
on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left')

### Add temp diff column
isd_data = isd_data.with_columns([
    (pl.col("Mean Air Temp.") - pl.col("Mean Dew Pt")).alias('Mean Temp Diff')
])

isd = isd_data.to_pandas()

### Add RH column

RH = []
metpy_RH = []

for i in range(0,isd.shape[0]):
    num = math.exp(17.625 * isd["Mean Dew Pt"].loc[i]/(243.04 + isd["Mean Dew Pt"].loc[i]))
    den = math.exp(17.625 * isd["Mean Air Temp."].loc[i]/(243.04 + isd["Mean Air Temp."].loc[i]))
    metpy_rh = 100*metpy.calc.relative_humidity_from_dewpoint(isd["Mean Air Temp."].loc[i] * units.degC, isd["Mean Dew Pt"].loc[i] * units.degC)
    out = 100 * (num/den)
    
    RH.append(out)
    metpy_RH.append(float(metpy_rh))
    
isd["Calculated RH"] = RH    
isd["MetPy RH"] = metpy_RH

isd.to_parquet('s3://ncai-humidity/isd/daily/'+str(mystate)+"-"+str(myyr)+'-DAILY.parquet',index=False)

isd.to_parquet('s3://ncai-humidity/isd/daily/'+str(mystate)+"-ALLYEARS-DAILY.parquet",index=False)

### END


