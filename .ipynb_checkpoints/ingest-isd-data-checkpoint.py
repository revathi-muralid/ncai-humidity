# Created on: 11/9/22 by RM
# Last updated: 11/21/22 by RM
# Purpose: To explore NOAA Integrated Surface Database (ISD) in situ humidity data

# ISD consists of global hourly observations compiled from an array of different sources
# The data are stored in the S3 bucket s3://noaa-isd-pds in fixed with text file format

# Import libraries
import awswrangler as wr
import polars as pl
import pyarrow.dataset as ds
import pyNCEI as ncei
from ncei.ISD import reader

# Athena is a package with the ability to query over S3 bucket objects, so this wil be used to work with ISD data in manageable chunks
from pyathena import connect

import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

# We will store our queried data in the athena_data_bucket object

athena_data_bucket = "isd-demo-"
conn = connect(s3_staging_dir="s3://" + athena_data_bucket + '-' + str(uuid.uuid4()),
               region_name=boto3.session.Session().region_name)

station_info = wr.s3.read_csv(
    path='s3://noaa-isd-pds/data/2022/A51256-00451-2022.gz',
    compression = 'gzip'
)