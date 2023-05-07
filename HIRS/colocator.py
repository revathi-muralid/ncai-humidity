# Created on: 5/4/23 by RM
# Last updated: 5/6/23 by RM

# Import libraries
import netCDF4
from netCDF4 import Dataset, num2date
import numpy as np
import pyarrow
import os
import boto3
from netcdfwrangler import hirsDat
import pyarrow.dataset as ds
import awswrangler as wr
import pandas as pd
import datetime as dt

# This script takes the timepoint associated with the spatial location in the satellite file, which we will call satloc_time
# Find closest time measurement in each ISD file to satloc_times in the matched data files; ideally want <1 hour apart
# Take each isd_loc above and find all instances in 3209 matched files where isd_loc has a satloc match

# Object: isd_loc

locs = wr.s3.read_parquet(path='s3://ncai-humidity/had-isd/hadley_stations.parquet') # MyVar class attribute

class MatchTime(object):

	locs = wr.s3.read_parquet(path='s3://ncai-humidity/had-isd/hadley_stations.parquet')
	
	def __init__(self, locind):
		self.dat = locs
		self.locind = locind

	# Need to run for every row of locs
	def get_isd(self):
		self.isdname = self.dat['stn_id'][self.locind]		
		self.isd_filepath = "s3://ncai-humidity/had-isd/hourly/pq/"+self.isdname+".parquet"
		self.isddf = wr.s3.read_parquet(path=self.isd_filepath)
		self.isddf['Time'] = self.isddf['time'].apply(lambda x: x.timestamp())
		self.isddf['Time'] = self.isddf['Time'].astype(float)
		return self.isddf

	# We want to get_hirs_time by scanning every single hirs file for ISD[locind]
	def get_hirs(self, hirsind):
		
		# Get list of HIRS filenames from appropriate s3 bucket
		self.s3 = boto3.resource('s3')
		self.objects = self.s3.Bucket(name='ncai-humidity').objects.filter(Prefix='matching/HIRS').all()
		self.hirs_fnames = []
		for i in self.objects:
			self.hirs_fnames.append(i.key)
		
		self.hirsind = hirsind
		self.hirs_path = "s3://ncai-humidity/"+self.hirs_fnames[self.hirsind]
		self.hirsdf = wr.s3.read_parquet(path = self.hirs_path)
		self.hirsdf.replace(['nan'],np.nan,inplace=True)
		self.hirs_sub = self.hirsdf[self.hirsdf['ISD_ID']==self.isdname]
		return self.hirs_sub

	# Run for every single HIRS/ISD matched file!
	# Save sattimes from every matched file to a list
	def hirs_time(self):
		self.hirs_times = self.hirs_sub.Time.values[0]
		if pd.isnull(self.hirs_times):
			print("There is no time in the ISD data associated with that HIRS pixel!")
		elif ' ' in self.hirs_times:
			x = self.hirs_sub['Time'].to_string()
			x = x.replace('1    [', '')
			x = x.replace(']', '')
			self.sattimes = x.split(' ')
			self.sattimes = np.array(self.sattimes).astype(float)
			self.sattime_min = float(np.min(self.sattimes))-(60*60)
			self.sattime_max = float(np.max(self.sattimes))+(60+60)
			self.sattime_list = []
			return self.sattimes
		else:
			self.sattimes = self.hirs_times#_sub['Time'].array
			self.sattimes = float(self.sattimes)
			self.sattime_min = float(np.min(self.sattimes))-(60*60)
			self.sattime_max = float(np.max(self.sattimes))+(60+60)
			self.sattime_list = []
			return self.sattimes

	# This gets us the times in the original ISD station data files that correspond to the times the HIRS satellite pixels were captured that matched with the ISD station in question
	def isd_time(self):
		self.isd_sub = self.isddf[self.isddf['Time']<=self.sattime_max]
		self.isd_sub = self.isd_sub[self.isd_sub['Time']>=self.sattime_min]
		return self.isd_sub['Time']			
mydir = "/store/hirsi/prof_L2/v5/ncdf_M02/"
#
#	mydat = hirsDat(filename, mydir)
#	print(mdat.fn)		
#	mydat.load(variables)
#	stn_list = []
#	time_list = []
#	x_list = []
#	y_list = []
#	hlon_list = []
#	hlat_list = []
#	ilon_list = []
#	ilat_list = []
#	dmin_list = []
#	for j in range(0,len(locs)):
#		in_lat = locs['lat'][j]
#		in_lon = locs['lon'][j]
#		mydat.calc_dmin(in_lon, in_lat) # Calculate min distance between HadISD station in question and every lon/lat in HIRS file
#		#mydat.get_dmin_vars()
#		stn_list.append(locs['stn_id'][j])
#		ilon_list.append(in_lon)
#		ilat_list.append(in_lat)
#		if len(mydat.dmin)==0:
#			dmin_list.append(np.nan)
#			time_list.append(np.nan)
#			x_list.append(np.nan)
#			y_list.append(np.nan)
#			hlon_list.append(np.nan)
#			hlat_list.append(np.nan)
#		elif len(mydat.dmin)==1:
#			dmin_list.append(np.ma.getdata(mydat.dmin))
#			ftime = mydat.dmin_time.astype(float)[0]
#			#out_time = dt.datetime.fromtimestamp(ftime)
#			time_list.append(ftime)
#			x_list.append(mydat.dmin_indx[0].astype(int))
#			y_list.append(mydat.dmin_indy[0].astype(int))
#			hlon_list.append(mydat.dmin_lon)
#			hlat_list.append(mydat.dmin_lat)
#		else:
#			dmin_list.append(np.ma.getdata(mydat.dmin).ravel().astype(int))
#			#print("there are multiple dmins: ",dmin_list.dtype)
#			ftime = np.ma.getdata(mydat.dmin_time).ravel().astype(float)
#			#out_time = [dt.datetime.fromtimestamp(ftime[i][0]) for i in range(len(ftime))]
#			time_list.append(ftime)
#			x_list.append(mydat.dmin_indx)
#			y_list.append(mydat.dmin_indy)
#			hlon_list.append(mydat.dmin_lon)
#			hlat_list.append(mydat.dmin_lat)
#	#dmin_list.append(mydat.dmin)
#	#print("Time List: ",time_list)
#	#dmin_list = np.reshape(dmin_list,-1,len(locs))
#	df = pd.DataFrame({'ISD_ID':stn_list,'Time':time_list, 'X':x_list, 'Y': y_list, 'ISD Lon': ilon_list, 'ISD Lat': ilat_list, 'HIRS Lon': hlon_list, 'HIRS Lat': hlat_list, 'DMin': dmin_list})
#	print(df[50:60])
#	outname = "s3://ncai-humidity/matching/HIRS/"+mydat.fname[:-7]+".parquet"
#	df['HIRS Lon'] = pd.Series(df['HIRS Lon'])#.str[0]
#	df['HIRS Lat'] = pd.Series(df['HIRS Lat'])#.str[0]
#	df['X'] = pd.Series(df['X'])
#	df['Y'] = pd.Series(df['Y'])
#	df['DMin'] = pd.Series(df['DMin'])
#	df = df.reset_index(drop=True)
#	df.astype(str).to_parquet(outname)



