# Created on: 4/7/23 by RM
# Last updated: 4/7/23 by RM

# Import libraries
import netCDF4
from netCDF4 import Dataset, num2date
import xarray as xr
import zarr
import s3fs
from glob import glob
import numpy as np
import pyarrow
import os
from hirswrangler import hirsDat
import pyarrow.dataset as ds
import awswrangler as wr
# Get bounding box

# NW corner: 36.4996,-94.617919
# NE corner: 39.466012,-75.242266
# SW corner: 28.928609,-94.043147
# SE corner: 24.523096,-80.031362
# AR | Arkansas | -94.617919 | 33.004106 | -89.644395 | 36.4996 |
# VA | Virginia | -83.675395 | 36.540738 | -75.242266 | 39.466012 |
# FL | Florida | -87.634938 | 24.523096 | -80.031362 | 31.000888 |
# LA | Louisiana | -94.043147 | 28.928609 | -88.817017 | 33.019457 |

# dat = wr.s3.read_parquet(path='s3://ncai-humidity/had-isd/Hadley_ISD_ALL_clean.parquet')

locs = wr.s3.read_parquet(path='s3://ncai-humidity/had-isd/hadley_stations.parquet')

min_lon = -94.617919
min_lat = 24.523096
max_lon = -75.242266
max_lat = 39.466012

variables = [
'time',
'lat',
'lon',
'surface_pressure',
'atmospheric_temperature',
'atmospheric_specific_humidity',
'surface_specific_humidity',
'flag_cld'
]

mydir = "/store/hirsi/prof_L2/v5/ncdf_M02/"

print(len(os.listdir(mydir)))

for i in range(2570,3209):
	filename = os.listdir(mydir)[i]
	mydat = hirsDat(filename, mydir)
	print(mydat.fn)		
	mydat.load(variables, min_lon, max_lon, min_lat, max_lat)
	#mydat.export
# dict_keys(['Y', 'X', 'pressure_levels_temp', 'pressure_levels_humidity', 'time', 'lat', 'lon', 'surface_pressure', 'atmospheric_temperature', 'atmospheric_specific_humidity', 'surface_temperature', 'air_temperature', 'surface_specific_humidity', 'flag_cld', 'flag_inv', 'solar_zenith_angle', 'sensor_zenith_angle'])

# Variables to get: air temperature (2m), lat, lon, atmospheric specific humidity, surface specific humidity, surface pressure, flag_cld

