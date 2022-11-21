# Created on: 11/9/22 by RM
# Last updated: 11/21/22 by RM
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
data = reader.read( year = [2010], country=['US'], state=['NC'] )

