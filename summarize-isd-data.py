# Created on: 12/9/22 by RM
# Last updated: 12/20/22 by RM
# Purpose: To summarize and explore NOAA Integrated Surface Database (ISD) in situ humidity data

# ISD consists of global hourly observations compiled from an array of different sources
# The data are stored in the S3 bucket s3://noaa-isd-pds in fixed with text file format

# FIRST: run 'pip install s3fs'
# pip install missingno

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

# Load years of interest
study_years = list(range(2000,2022))

myvars = ["('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('wind', 'speed QC')","('air temperature', 'temperature')","('air temperature', 'temperature QC')","('dew point', 'temperature')","('sea level pressure', 'pressure')","('sea level pressure', 'pressure QC')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')","('Relative-Humidity-Raw 1', 'RH_PERIOD')","('Relative-Humidity-Raw 2', 'RH_PERIOD')","('Relative-Humidity-Raw 3', 'RH_PERIOD')","('Relative-Humidity-Temperature 1', 'RH_T_PERIOD')","('Relative-Humidity-Temperature 1', 'T_AVE')","('Relative-Humidity-Temperature 1', 'RH_AVE')","('Hourly-RH-Temperature 1', 'MIN_RH_T')","('Hourly-RH-Temperature 1', 'MAX_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH_T')","('Hourly-RH-Temperature 1', 'SD_RH')", "('Relative-Humidity-Raw 1', 'RH_QC')", "('Relative-Humidity-Raw 2', 'RH_QC')","('Relative-Humidity-Raw 3', 'RH_QC')", "('Relative-Humidity-Temperature 1', 'T_AVE_QC')","('Relative-Humidity-Temperature 1', 'T_AVE_QC_FLG')", "('Relative-Humidity-Temperature 1', 'RH_AVE_QC')","('Relative-Humidity-Temperature 1', 'RH_AVE_QC_FLG')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC')","('Hourly-RH-Temperature 1', 'MIN_RH_T_QC_FLG')", "('Hourly-RH-Temperature 1', 'MAX_RH_T_QC')","('Hourly-RH-Temperature 1', 'MAX_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC')","('Hourly-RH-Temperature 1', 'SD_RH_T_QC_FLG')","('Hourly-RH-Temperature 1', 'SD_RH_QC')","('Hourly-RH-Temperature 1', 'SD_RH_QC_FLG')"]

subvars = []

# Scan in station data
queries = []
for year in study_years:
    q = (
        ds.dataset("s3://ncai-humidity/isd/NC-"+str(year)+".parquet", partitioning='hive')
    )
    queries.append(q)

isd_data = (
    pl.from_arrow(
        ds.dataset(queries)
        .scanner(
            columns = myvars#,
            #filter = ds.field('ELEMENT').isin(elements)
        )
        .to_table()
     )
    # .with_column(
    #     pl.col('DATE').str.strptime(pl.Date, "%Y%m%d")
    # )
    # .filter(pl.col('DATE') >= pl.datetime(2020,1,1))
    # .groupby(['DATE', 'ELEMENT'])
    # .agg(
    #     pl.col('DATA_VALUE').mean()
    # )
    # .pivot(
    #     values = 'DATA_VALUE', 
    #     index = 'DATE', 
    #     columns = 'ELEMENT'
    # )    
)

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

# Summarize completeness of data

msno.matrix(dat0[["('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('air temperature', 'temperature')","('dew point', 'temperature')","('sea level pressure', 'pressure')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')"]])
plt.show()

grouped = dat10.groupby("('Control', 'USAF')")
sizes = grouped.size().values * len(dat10.columns)

num_of_nans = sizes - grouped.count().sum(axis=1)
out = num_of_nans / sizes
out2 = out.to_frame().rename(columns={0: 'NaPercent'})












# Load years of interest
study_years = list(range(2000,2022))

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








# def to_parquet( outBucket, res, clobber=False, **kwargs ):
#   """
#   Store parsed ISD data in parquet format

#   Given an iterable of DataFrame(s) containing parsed data from 
#   ISD yearly file(s), save the data in a bucket on s3.

#   This is intended to be used as the 'joiner' function in a call
#   to s3download.download() so that as results (parsed data) from
#   ISD files become available, the data are converted and stored
#   in an s3 bucket.

#   Arguments:
#     outBucket (str) : Name of the bucket to save parquet file in
#     res (iterable) : Output from a multiprocessing.Pool() mapping

#   Keyword arguments:
#     clobber (bool) : If set, will delete/overwrite any existing parquet
#       files with matching name/key
#     **kwargs : passed directly to the createBucket() function.

#   Returns:
#     None.
    
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
