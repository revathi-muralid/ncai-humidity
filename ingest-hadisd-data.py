
conda install -c conda-forge iris
conda install -c conda-forge nctoolkit
conda install -c conda-forge cdo
conda update -n base -c conda-forge conda
conda install -c conda-forge nco

import os 
import iris
import iris.quickplot as qplt 
import matplotlib.pyplot as plt
import pandas as pd
import requests
import urllib
import netCDF4
from netCDF4 import Dataset
import gzip
import tempfile
import shutil
import xarray as xr
import numpy.ma as ma
import cdo
import nco
import nctoolkit as nc

# Get US station IDs from HadISD station list
path_to_file = 'hadisd_station_info_v330_2022f.txt'

df = pd.read_table(path_to_file, sep="\s+", header=None)

new_df = df[~df[0].str.contains("99999")]
stations = new_df[new_df[0].str[0:1].isin(['7'])]

stn_id = "700260-27502"
url = "https://www.metoffice.gov.uk/hadobs/hadisd/v330_2022f/data/hadisd.3.3.0.2022f_19310101-20230101_"+stn_id+".nc.gz"
new = stn_id+".nc.gz"
urllib.request.urlretrieve(url, new)
old = stn_id+".nc"

with gzip.open(new, 'rb') as f_in:
    with open(old, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

infile = old
data = nc.open_data(infile)

dat10_11 = data.subset(years = [2010, 2011])

dat = data.to_dataframe()

ds = nc.open_url(url)

def open_netcdf(fname):
    if fname.endswith(".gz"):
        infile = gzip.open(fname, 'rb')
        tmp = tempfile.NamedTemporaryFile(delete=False)
        shutil.copyfileobj(infile, tmp)
        infile.close()
        tmp.close()
        data = nc.open_data(tmp.name)
        #data = netCDF4.Dataset(tmp.name,'r')
        #data = xr.open_dataset(tmp.name)
        os.unlink(tmp.name)
    else:
        data = nc.open_data(fname)
        #data = netCDF4.Dataset(fname,'r')
        #data = xr.open_dataset(fname)
    return data

dat = open_netcdf(new)

print(dat.variables.keys()) 
# dict_keys(['longitude', 'latitude', 'elevation', 'station_id', 'temperatures', 'dewpoints', 'slp', 'stnlp', 'windspeeds', 'winddirs', 'total_cloud_cover', 'low_cloud_cover', 'mid_cloud_cover', 'high_cloud_cover', 'precip1_depth', 'precip2_depth', 'precip3_depth', 'precip6_depth', 'precip9_depth', 'precip12_depth', 'precip15_depth', 'precip18_depth', 'precip24_depth', 'cloud_base', 'wind_gust', 'past_sigwx1', 'time', 'input_station_id', 'quality_control_flags', 'flagged_obs', 'reporting_stats'])

ds = xr.open_dataset(new)
df = dat.to_dataframe()

lon = ma.asarray(dat.variables['longitude'][:])
lon_flat = lon.ravel()
lon_flat = lon_flat.reshape(lon_flat.size, 1)
lat = ma.asarray(dat.variables['latitude'][:])
lat_flat = lat.ravel()
lat_flat = lat_flat.reshape(lat_flat.size, 1)
elev = ma.asarray(dat.variables['elevation'][:])
elev_flat = elev.ravel()
elev_flat = elev_flat.reshape(elev_flat.size, 1)
myid = ma.asarray(dat.variables['station_id'][:])
myid_flat = myid.ravel()
myid_flat = myid_flat.reshape(myid_flat.size, 1)
temp = ma.asarray(dat.variables['temperatures'][:])
temp_flat = temp.ravel()
temp_flat = temp_flat.reshape(temp_flat.size, 1)
dpt = ma.asarray(dat.variables['dewpoints'][:])
dpt_flat = dpt.ravel()
dpt_flat = dpt_flat.reshape(dpt_flat.size, 1)
slp = ma.asarray(dat.variables['slp'][:])
slp_flat = slp.ravel()
slp_flat = slp_flat.reshape(slp_flat.size, 1)
stnlp = ma.asarray(dat.variables['stnlp'][:])
stnlp_flat = stnlp.ravel()
stnlp_flat = stnlp_flat.reshape(stnlp_flat.size, 1)
ws = ma.asarray(dat.variables['windspeeds'][:])
ws_flat = ws.ravel()
ws_flat = ws_flat.reshape(ws_flat.size, 1)
time = ma.asarray(dat.variables['time'][:])
time_flat = time.ravel()
time_flat = time_flat.reshape(time_flat.size, 1)
qc_flags = ma.asarray(dat.variables['quality_control_flags'][:])
qc_flat = qc_flags.ravel()
qc_flat = qc_flat.reshape(qc_flat.size, 1)
flg_obs = ma.asarray(dat.variables['flagged_obs'][:])
flg_flat = flg_obs.ravel()
flg_flat = flg_flat.reshape(flg_flat.size, 1)

output = ma.column_stack((temp_flat,dpt_flat,slp_flat,stnlp_flat,ws_flat,time_flat,qc_flat,flg_flat))

mask = time[time == 806471.]

