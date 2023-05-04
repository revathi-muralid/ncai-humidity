# Created on: 4/25/23 by RM
# Last updated: 5/1/23 by RM

# Import libraries
import netCDF4
from netCDF4 import Dataset, num2date
import s3fs
import datetime as dt
import numpy as np
import pyarrow
import os
import math
from scipy import spatial

class hirsDat:
	def __init__(self, fname, mydir):
		self.fname = fname
		self.fn = os.path.join(mydir, fname)

	def load(self, myvars):
		self.dat = netCDF4.Dataset(self.fn)
		self.lats = self.dat.variables['lat'][:] # shape: 56,5429
		self.lats = self.lats.filled(fill_value=np.nan)
		self.lons = self.dat.variables['lon'][:] # shape: 56,5429
		#self.lons[self.lons==-32678] = np.nan
		self.lons = self.lons.filled(fill_value=np.nan)
		self.time = self.dat.variables['time'][:] # shape: 56, 5429
		self.sfc_press_ao = self.dat.variables['surface_pressure'].add_offset
		self.sfc_press_sf = self.dat.variables['surface_pressure'].scale_factor
		self.sfc_press = self.dat.variables['surface_pressure'][:] * self.sfc_press_sf + self.sfc_press_ao
		self.atm_temp_ao = self.dat.variables['atmospheric_temperature'].add_offset
		self.atm_temp_sf = self.dat.variables['atmospheric_temperature'].scale_factor
		self.atm_temp = self.dat.variables['atmospheric_temperature'][:] * self.atm_temp_sf + self.atm_temp_ao
		self.air_temp_ao = self.dat.variables['air_temperature'].add_offset
		self.air_temp_sf = self.dat.variables['air_temperature'].scale_factor
		self.air_temp = self.dat.variables['air_temperature'][0] * self.air_temp_sf + self.air_temp_ao
		self.atm_sh_ao = self.dat.variables['atmospheric_specific_humidity'].add_offset
		self.atm_sh_sf = self.dat.variables['air_temperature'].scale_factor
		self.atm_sh = self.dat.variables['atmospheric_specific_humidity'][:] * self.atm_sh_sf + self.atm_sh_ao
		self.sfc_sh_ao = self.dat.variables['surface_specific_humidity'].add_offset
		self.sfc_sh_sf = self.dat.variables['surface_specific_humidity'].scale_factor
		self.sfc_sh = self.dat.variables['surface_specific_humidity'][:] * self.sfc_sh_sf + self.sfc_sh_ao
		self.flag_cld = self.dat.variables['flag_cld'][:]
	
	def calc_dmin(self, in_lon, in_lat):
		self.dists = np.sqrt(abs( (self.lons - in_lon)**2 + (self.lats - in_lat)**2 ) )
		self.dmin = self.dists[self.dists<=0.1414]
		if len(self.dmin)==0:
			self.dmin_ind = np.nan
			self.dmin_indx = np.nan
			self.dmin_indy = np.nan
			self.dmin_time = np.nan
			self.dmin_lat = np.nan
			self.dmin_lon = np.nan
		elif len(self.dmin)==1:
			self.dmin_ind = np.where(self.dists==self.dmin)
			self.dmin_indx = self.dmin_ind[0].astype(int)
			self.dmin_indy = self.dmin_ind[1].astype(int)
			self.dmin_time = self.time[self.dmin_indx,self.dmin_indy]
			self.dmin_lat = self.lats[self.dmin_indx,self.dmin_indy].astype(float)
			self.dmin_lon = self.lons[self.dmin_indx,self.dmin_indy].astype(float)
		else:
			self.dmin_ind = []
			for i in range(len(self.dmin)):
				val = np.where(self.dists==self.dmin[i])
				if np.isnan(self.dmin[i])==True:
					continue
				else:
					self.dmin_ind.append(val)
					#self.dmin_ind = np.array(self.dmin_ind, dtype=object)
					#self.dmin_indx = [self.dmin_ind[i,0][0] for i in range(len(self.dmin_ind))]
					#self.dmin_indy = [self.dmin_ind[i,1][0] for i in range(len(self.dmin_ind))]	
				#self.dmin_ind.append(val)
			self.dmin_ind = np.array(self.dmin_ind, dtype=object)
			self.dmin_indx = [self.dmin_ind[i,0][0] for i in range(len(self.dmin_ind))]
			self.dmin_indy = [self.dmin_ind[i,1][0] for i in range(len(self.dmin_ind))]
			self.dmin_time = [self.time[self.dmin_indx[i],self.dmin_indy[i]] for i in range(len(self.dmin_ind))]
			self.dmin_lat = self.lats[self.dmin_indx, self.dmin_indy].astype(float)
			self.dmin_lon = self.lons[self.dmin_indx, self.dmin_indy].astype(float)

	def get_dmin_vars(self):
		if len(self.dmin)==0:
			self.dmin_time = np.nan
			self.dmin_lat = np.nan
			self.dmin_lon = np.nan
		elif len(self.dmin)==1:
			self.dmin_time = self.time[self.dmin_indx,self.dmin_indy]
			self.dmin_lat = self.lats[self.dmin_indx,self.dmin_indy].astype(float)
			self.dmin_lon = self.lons[self.dmin_indx,self.dmin_indy].astype(float)
		else:
			#self.dmin_ind = np.array(self.dmin_ind)
			#print("Time before: ",self.time)
			self.dmin_time = [self.time[self.dmin_indx[i],self.dmin_indy[i]] for i in range(len(self.dmin_ind))]
			self.dmin_lat = self.lats[self.dmin_indx, self.dmin_indy].astype(float)
			self.dmin_lon = self.lons[self.dmin_indx, self.dmin_indy].astype(float)
			print(self.dmin_lat[np.isnan(self.dmin_lat)==True])
			#self.dmin_time = self.dmin_time[np.isnan(self.dmin_time)==False]
			self.dmin_lat = self.dmin_lat[np.isnan(self.dmin_lat)==False]
			self.dmin_lon = self.dmin_lon[np.isnan(self.dmin_lon)==False]	
		#self.ds3 = self.ds2[myvars]
	
		#if (self.ds3.dims["across_track"]==0 | self.ds3.dims["along_track"]==0):
		#	print(self.fname+" is unusable!")
		#else:
		#	self.new_fname = self.fname[:-3]
		#	self.new_fn = 's3://ncai-humidity/HIRS/SE_'+self.new_fname+'.zarr'
		#	print(self.new_fn)
		#	self.ds3.to_zarr(self.new_fn, mode='w')		

#dict_keys(['Y', 'X', 'pressure_levels_temp', 'pressure_levels_humidity', 'time', 'lat', 'lon', 'surface_pressure', 'atmospheric_temperature', 'atmospheric_specific_humidity', 'surface_temperature', 'air_temperature', 'surface_specific_humidity', 'flag_cld', 'flag_inv', 'solar_zenith_angle', 'sensor_zenith_angle'])

# Variables to get: air temperature (2m), lat, lon, atmospheric specific humidity, surface specific humidity, surface pressure, flag_cld

