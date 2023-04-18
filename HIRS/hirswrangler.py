# Created on: 4/18/23 by RM
# Last updated: 4/18/23 by RM

# Import libraries
import netCDF4
from netCDF4 import Dataset, num2date
import xarray as xr
import zarr
import s3fs
import numpy as np
import pyarrow
import os

class hirsDat:
	def __init__(self, fname, mydir):
		self.fname = fname
		self.fn = os.path.join(mydir, fname)

	def load(self, myvars, min_lon, max_lon, min_lat, max_lat):
		self.ds = xr.open_dataset(self.fn, engine="netcdf4")
		self.ds.load()

		self.mask_lon = (self.ds.lon >= min_lon) & (self.ds.lon <= max_lon)
		self.mask_lat = (self.ds.lat >= min_lat) & (self.ds.lat <= max_lat)

		self.ds2 = self.ds.where(self.mask_lon & self.mask_lat, drop=True)
		self.ds2.load()

		self.ds3 = self.ds2[myvars]
		self.ds3.load()
		print(self.ds3.dims)

		if (self.ds3.dims["across_track"]==0 | self.ds3.dims["along_track"]==0):
			print(self.fname+" is unusable!")
		else:
			self.new_fname = self.fname[:-3]
			self.new_fn = 's3://ncai-humidity/HIRS/SE_'+self.new_fname+'.zarr'
			print(self.new_fn)
			self.ds3.to_zarr(self.new_fn, mode='w')		

#dict_keys(['Y', 'X', 'pressure_levels_temp', 'pressure_levels_humidity', 'time', 'lat', 'lon', 'surface_pressure', 'atmospheric_temperature', 'atmospheric_specific_humidity', 'surface_temperature', 'air_temperature', 'surface_specific_humidity', 'flag_cld', 'flag_inv', 'solar_zenith_angle', 'sensor_zenith_angle'])

# Variables to get: air temperature (2m), lat, lon, atmospheric specific humidity, surface specific humidity, surface pressure, flag_cld

