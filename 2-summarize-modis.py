
# Created on: 4/11/23 by RM
# Last updated: 4/11/23 by RM

import pandas as pd
import os
import awswrangler as wr
import s3fs
import boto3
from numpy import *
import numpy as np
from datetime import datetime
from datetime import timedelta

fname = 'MOD07_L2.A2000153.1640.061.2017207224921'
test = pd.read_parquet("s3://ncai-humidity/MODIS/MOD07_L2/" + fname + ".parquet")
