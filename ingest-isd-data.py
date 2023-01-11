# Created on: 11/9/22 by RM
# Last updated: 1/10/22 by RM
# Purpose: To explore NOAA Integrated Surface Database (ISD) in situ humidity data

# ISD consists of global hourly observations compiled from an array of different sources
# The data are stored in the S3 bucket s3://noaa-isd-pds in fixed with text file format

### FIRST: Run 'pip install ../pyncei'

### THEN: go through the rest!

# Import libraries
import awswrangler as wr
import polars as pl
import pyarrow.dataset as ds
import ncei
import boto3
import us
import s3fs
import numpy as np
from ncei.ISD import reader
from ncei.ISD.parsers import parseData
from ncei.ISD.parsers.additional import FieldParser, AdditionalParser, parseAdditional
from ncei.ISD import convert
from functools import partial

ISD_BUCKET = 'noaa-isd-pds'

### Modify code to also pull RH and vapor pressure along with QC variables

# Build parser for atmospheric pressure data
atm = AdditionalParser('MA[1]', 'Atmospheric-Pressure-Observation',
                          FieldParser(    'ALTIM_SET_RATE', 5, int, min=8635, max=10904, scale_factor= 10, missing=  99999 , units='hPa'),
                          FieldParser(     'ALTIM_QC', 1, str),
                          FieldParser( 'PRESS_RATE', 5, int, min=4500, max=10900, scale_factor= 10, missing=  99999 , units='hPa'),
                          FieldParser(        'ATM_PRESS_QC', 1, str )
)

# Build parser for raw relative humidity data
rh_raw = AdditionalParser('RH[1-3]', 'Relative-Humidity-Raw',
                          FieldParser('RH_PERIOD', 3, int, min=1, max=744, scale_factor= 1, missing=999 , units='hours'),
                          FieldParser('RH_CODE', 1, str),
                          FieldParser( 'RH_PERC', 3, int, min=0, max=100, scale_factor= 1, missing=  999 , units='percent'),
                          FieldParser('RH_DERIV_CODE', 1, str),
                       FieldParser('RH_QC', 1, str)
)

# Build parser for average relative humidity data
rh_ave = AdditionalParser('CH[1-2]', 'Relative-Humidity-Temperature',
                          FieldParser('RH_T_PERIOD', 2, int, min=0, max=60, scale_factor= 1, missing=99 , units='minutes'),
                          FieldParser('T_AVE', 5, int, min=-9999, max=9998, scale_factor= 1, missing=  99 , units='degC'),
                          FieldParser( 'T_AVE_QC', 1, str),
                          FieldParser('T_AVE_QC_FLG', 1, str),
                       FieldParser('RH_AVE', 4, int, min=0, max=1000, scale_factor= 10, missing=  9999 , units='percent'),
                          FieldParser('RH_AVE_QC', 1, str),
                          FieldParser( 'RH_AVE_QC_FLG', 1, str)
)

# Build parser for hourly relative humidity data
hrly_rh = AdditionalParser('CI[1]', 'Hourly-RH-Temperature',
                          FieldParser('MIN_RH_T', 5, int, min=-9999, max=9999, scale_factor= 10, missing=9999 , units='degC'),
                          FieldParser('MIN_RH_T_QC', 1, str),
                          FieldParser( 'MIN_RH_T_QC_FLG', 1, str),
                          FieldParser('MAX_RH_T', 5, int, min=-9999, max=9998, scale_factor = 10, missing=9999, units='degC'),
                       FieldParser('MAX_RH_T_QC', 1, str),
                          FieldParser('MAX_RH_T_QC_FLG', 1, str),
                          FieldParser( 'SD_RH_T', 5, int, min=0, max=99998, scale_factor = 10, missing = 9999, units='NA'),
                           FieldParser( 'SD_RH_T_QC', 1, str),
                           FieldParser( 'SD_RH_T_QC_FLG', 1, str),
                           FieldParser( 'SD_RH', 5, int, min=0, max=99998, scale_factor= 10, missing=99999 , units='NA'),
                           FieldParser( 'SD_RH_QC', 1, str),
                           FieldParser( 'SD_RH_QC_FLG', 1, str)
)

# Build parser for variables to join on
hrly_rh = AdditionalParser('CI[1]', 'Hourly-RH-Temperature',
                          FieldParser('MIN_RH_T', 5, int, min=-9999, max=9999, scale_factor= 10, missing=9999 , units='degC'),
                          FieldParser('MIN_RH_T_QC', 1, str),
                          FieldParser( 'MIN_RH_T_QC_FLG', 1, str),
                          FieldParser('MAX_RH_T', 5, int, min=-9999, max=9998, scale_factor = 10, missing=9999, units='degC'),
                       FieldParser('MAX_RH_T_QC', 1, str),
                          FieldParser('MAX_RH_T_QC_FLG', 1, str),
                          FieldParser( 'SD_RH_T', 5, int, min=0, max=99998, scale_factor = 10, missing = 9999, units='NA'),
                           FieldParser( 'SD_RH_T_QC', 1, str),
                           FieldParser( 'SD_RH_T_QC_FLG', 1, str),
                           FieldParser( 'SD_RH', 5, int, min=0, max=99998, scale_factor= 10, missing=99999 , units='NA'),
                           FieldParser( 'SD_RH_QC', 1, str),
                           FieldParser( 'SD_RH_QC_FLG', 1, str)
)

myParser = partial(parseAdditional, parsers=[atm,rh_raw,rh_ave,hrly_rh] )
parser   = partial( parseData, parser = myParser )

### NOTE: Only 2008 and beyond have data for all parsers.

myyr = 2022

data     = reader.read( year=[myyr], country=['US'], state=['NC'], parser=parser )

data0     = reader.read( year=[myyr], country=['US'], state=['NC'])

### CHECK DEW PT VS AIR TEMP DIFFS

dat = data0.iloc[:,[23,25]]

dat.columns = ["air_temp", "dew_pt"]

diff = dat["air_temp"].sub(dat["dew_pt"], axis=0)

np.mean(diff)

### IF THERE IS A DIFFERENCE MOVE ON TO NEXT STEP

isd_data = data0.join(data)

for col in isd_data.columns:
    print(col)

isd_data.to_parquet('s3://ncai-humidity/isd/NC-'+str(myyr)+'.parquet',index=False)

### END



data     = reader.read( year=[2010], country=['US'], state=['NC'], parser=myParser, joiner=[reader.joiner,])




dat = data0.iloc[:,[23, 25]]
df_new = dat[dat.columns.difference(["('air temperature', 'temperature')","('dew point', 'temperature')"])]

dat2 = dat.diff(axis=1)
dat2 =  dat2[dat2.iloc[:,0].notnull()]


dat = data0[[1:2]]
             
for col in data0.columns:
    print(col)

data.shape

data0.shape





#### SCRATCH


myParsers = [atm,rh_raw,rh_ave,hrly_rh]
convert.humid_convert('ncai-humidity', 2010,2010, myParsers, country=['US'], state=['RI'], isd_bucket=ISD_BUCKET)