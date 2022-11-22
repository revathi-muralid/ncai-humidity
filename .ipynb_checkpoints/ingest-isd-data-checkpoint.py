# Created on: 11/9/22 by RM
# Last updated: 11/22/22 by RM
# Purpose: To explore NOAA Integrated Surface Database (ISD) in situ humidity data

# ISD consists of global hourly observations compiled from an array of different sources
# The data are stored in the S3 bucket s3://noaa-isd-pds in fixed with text file format

# Import libraries
import awswrangler as wr
import polars as pl
import pyarrow.dataset as ds
import pyncei as ncei
import boto3
from ncei.ISD import reader
from ncei.ISD.parsers import parseData
from ncei.ISD.parsers.additional import FieldParser, AdditionalParser, parseAdditional
from functools import partial

### Modify code to also pull RH and vapor pressure along with QC variables

# Build parser for atmospheric pressure data
atm = AdditionalParser('MA[1]', 'Atmospheric-Pressure-Observation',
                          FieldParser(    'alti-set-rate', 5, int, min=8635, max=10904, scale_factor= 10, missing=  99999 , units='hPa'),
                          FieldParser(     'alti-qc', 1, str),
                          FieldParser( 'pressure-rate', 5, int, min=4500, max=10900, scale_factor= 10, missing=  99999 , units='hPa'),
                          FieldParser(        'atm-press-qc', 1, str )
)

# Build parser for raw relative humidity data
rh_raw = AdditionalParser('RH[1-3]', 'Relative-Humidity-Raw',
                          FieldParser('rh-period', 3, int, min=1, max=744, scale_factor= 1, missing=999 , units='hours'),
                          FieldParser('rh-code', 1, str),
                          FieldParser( 'rh-perc', 3, int, min=0, max=100, scale_factor= 1, missing=  999 , units='percent'),
                          FieldParser('rh-derived-code', 1, str),
                       FieldParser('rh-qc', 1, str)
)

# Build parser for average relative humidity data
rh_ave = AdditionalParser('CH[1-2]', 'Relative-Humidity-Temperature',
                          FieldParser('rh-temp-period', 2, int, min=0, max=60, scale_factor= 1, missing=99 , units='minutes'),
                          FieldParser('temp-ave', 5, int, min=-9999, max=9998, scale_factor= 1, missing=  99 , units='degC'),
                          FieldParser( 'temp-ave-qc', 1, str),
                          FieldParser('temp-ave-qc-flag', 1, str),
                       FieldParser('rh-ave', 4, int, min=0, max=1000, scale_factor= 10, missing=  9999 , units='percent'),
                          FieldParser('rh-ave-qc', 1, str),
                          FieldParser( 'rh-ave-qc-flag', 1, str)
)

# Build parser for hourly relative humidity data
hrly_rh = AdditionalParser('CI[1]', 'Hourly-RH-Temperature',
                          FieldParser('min-rh-temp', 5, int, min=-9999, max=9999, scale_factor= 10, missing=9999 , units='degC'),
                          FieldParser('min-rh-temp-qc', 1, str),
                          FieldParser( 'min-rh-temp-qc-flag', 1, str),
                          FieldParser('max-rh-temp', 5, int, min=-9999, max=9998, scale_factor = 10, missing=9999, units='degC'),
                       FieldParser('max-rh-temp-qc', 1, str),
                          FieldParser('max-rh-temp-qc-flag', 1, str),
                          FieldParser( 'rh-temp-sd', 5, int, min=0, max=99998, scale_factor = 10, missing = 9999, units='NA'),
                           FieldParser( 'rh-temp-sd-qc', 1, str),
                           FieldParser( 'rh-temp-sd-qc-flag', 1, str),
                           FieldParser( 'rh-sd', 5, int, min=0, max=99998, scale_factor= 10, missing=99999 , units='NA'),
                           FieldParser( 'rh-sd-qc', 1, str),
                           FieldParser( 'rh-sd-qc-flag', 1, str)
)

myParser = partial(parseAdditional, parsers=[rh_raw] )
parser   = partial( parseData, parser = myParser )
data     = reader.read( year=[2010], country=['US'], state=['CA'], parser=parser )

for col in data.columns:
    print(col)
