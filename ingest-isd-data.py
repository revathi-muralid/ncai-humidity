# Created on: 11/9/22 by RM
# Last updated: 11/30/22 by RM
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
from ncei.ISD import reader
from ncei.ISD.parsers import parseData
from ncei.ISD.parsers.additional import FieldParser, AdditionalParser, parseAdditional
from ncei.ISD import convert
from functools import partial

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

outputter = convert.to_parquet('ncai-humidity/ISD', res=res)

myParser = partial(parseAdditional, parsers=[atm,rh_raw,rh_ave,hrly_rh] )
parser   = partial( parseData, parser = myParser )

convert.humid_convert('ncai-humidity', 2010,2011, country=['US'], state=['RI'], isd_bucket=ISD_BUCKET)

data     = reader.read( year=[2010], country=['US'], state=['NC'], parser=parser, joiner=[reader.joiner,])
data0     = reader.scan( year=[2010], country=['US'], state=['NC'])

data     = reader.read( year=[2010], country=['US'], state=['NC'], parser=parser )
data0     = reader.read( year=[2010], country=['US'], state=['NC'])

isd_data = data0.join(data)
isd_df = isd_data.reset_index()

for col in isd_data.columns:
    print(col,isd_data[col].dtype)

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

isd_data.to_parquet('s3://ncai-humidity/isd/NC-2010.parquet',index=False)


# Make parser that converts to parquet at end
# toughest part is getting file name you wanna save as in parser function
# converter.py in ISD subpackage

wr.s3.to_parquet(
    isd_df,
    "s3://ncai-humidity/isd/NC-2010.parquet"
)





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
state_names = [state.abbr for state in us.states.STATES]
        
# Load years of interest
study_years = list(range(2000,2022))

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

