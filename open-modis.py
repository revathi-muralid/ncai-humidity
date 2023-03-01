import xarray
import rioxarray as rxr
import pandas as pd
import os
import awswrangler as wr
import s3fs
import boto3
import requests
import warnings
import pyhdf.SD as SD
from pyhdf.SD import SD, SDC

# Get list of filenames

filenames = pd.read_csv('LAADS_fnames_2000_22.csv')

myfiles = list(filenames['filename'])

# https://github.com/pandas-dev/pandas/issues/31902

s3 = boto3.client('s3')

f1 = 'MOD07_L2.A2000294.1740.061.2017213061441.hdf'

test = s3.get_object(Bucket='prod-lads', Key='MOD07_L2/MOD07_L2.A2000294.1740.061.2017213061441.hdf')

df=pd.read_hdf(test)

####################################################################

session = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], 
                        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                        aws_session_token=os.environ['AWS_SESSION_TOKEN'],
                        region_name='us-west-2')
s3 = boto3.resource('s3')
f1 = 'MOD07_L2.A2001117.1210.061.2017223080430.hdf'

s3.meta.client.download_file('prod-lads', 'MOD07_L2/%s'%f1, f1)

# Read in file
# https://geonetcast.wordpress.com/2021/03/26/reading-modis-terra-hdf-files-with-pyhdf/

hdf = SD(f1, SDC.READ)

# Print dataset names

ds_dic = hdf.datasets()

my_dic = dict((k, ds_dic[k]) for k in ('Latitude', 'Longitude', 'Scan_Start_Time','Cloud_Mask','Surface_Pressure','Surface_Elevation','Processing_Flag','Retrieved_Temperature_Profile','Retrieved_Moisture_Profile','Water_Vapor','Water_Vapor_Low','Water_Vapor_High','Quality_Assurance'))

vars = {}
for idx,sds in enumerate(my_dic.keys()):
    x = hdf.select(sds)
    xs = x.get()
    xs = pd.DataFrame(xs.flatten())
    vars[sds] = xs
    print (idx,sds)

# 270 5-km pixels in width
# 406 5-km pixels in length
# for nine consecutive granules
# every 10th granule has 
# Temp and moisture profiles are produced at 20 vertical levels
    
# 109620: All vars except the three below
# 1096200: Quality_Assurance
# 2192400: Ret_Temp_Profile, Ret_Moist_Profile - represents 20 levels of data; need just one layer of data



# Read dataset
lat = hdf.select('Latitude')
lon = hdf.select('Longitude')
st_time = hdf.select('Scan_Start_Time')
cloud = hdf.select('Cloud_Mask')
sur_press = hdf.select('Surface_Pressure')
sur_elev = hdf.select('Surface_Elevation')
proc_flag = hdf.select('Processing_Flag')
ret_temp = hdf.select('Retrieved_Temperature_Profile')
ret_moist = hdf.select('Retrieved_Moisture_Profile')
h2o_vap = hdf.select('Water_Vapor')
h2o_low = hdf.select('Water_Vapor_Low')
h2o_hi = hdf.select('Water_Vapor_High')
qa = hdf.select('Quality_Assurance')

lats=lat.get()
lons=lon.get()
st_times=st_time.get()
clouds=cloud.get()
sur_prs=sur_press.get()
sur_elevs=sur_elev.get()
proc_flags=proc_flag.get()
ret_temps=ret_temp.get()
ret_moists=ret_moist.get()
h2o_vaps=h2o_vap.get()
h2o_lows=h2o_low.get()
h2o_his=h2o_hi.get()
qas=qa.get()

lats = pd.DataFrame(lats.flatten())
lons = pd.DataFrame(lons.flatten())
clouds = pd.DataFrame(clouds.flatten())

#lons = lon[:,:]

os.remove(f1)



dat = rxr.open_rasterio(f1,masked=True)

### ATTEMPT ONE ###

s3 = s3fs.S3FileSystem(anon=False)
s3path = 's3://prod-lads/MOD07_L2/MOD07_L2.A2000068.2250.061.2017202230318.hdf'
remote_files = s3.glob(s3path)






modis_pre_path = os.path.join("MYD07_L2.A2023051.0945.061.2023051234722.hdf")
modis_pre = rxr.open_rasterio(modis_pre_path,
                              masked=True)
# ERROR: 
# RasterioIOError: 'MYD07_L2.A2023051.0945.061.2023051234722.hdf' not recognized as a supported file format.


### ATTEMPT TWO ###

s3 = boto3.client('s3')

s3.download_file('prod-lads','MOD07_L2','MOD07_L2.A2000068.2250.061.2017202230318.hdf')

with open('MOD07_L2.A2000068.2250.061.2017202230318.hdf', 'wb') as f:
    s3.download_fileobj('prod-lads', 'MOD07_L2', f)
    
myobjs = mybucket.objects.all()

### ATTEMPT THREE ###

testpath = 's3://prod-lads/MOD07_L2/MOD07_L2.A2000068.2250.061.2017202230318.hdf'

pd.read_hdf(testpath)




# {'Latitude': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 5, 0), 'Longitude': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 5, 1), 'Scan_Start_Time': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 6, 2), 'Solar_Zenith': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 3), 'Solar_Azimuth': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 4), 'Sensor_Zenith': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 5), 'Sensor_Azimuth': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 6), 'Brightness_Temperature': (('Band_Number:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (12, 406, 270), 22, 7), 'Cloud_Mask': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 20, 8), 'Skin_Temperature': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 9), 'Surface_Pressure': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 10), 'Surface_Elevation': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 11), 'Processing_Flag': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 20, 12), 'Tropopause_Height': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 13), 'Guess_Temperature_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 14), 'Guess_Moisture_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 15), 'Retrieved_Temperature_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 16), 'Retrieved_Moisture_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 17), 'Retrieved_WV_Mixing_Ratio_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 18), 'Retrieved_Height_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 19), 'Total_Ozone': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 20), 'Total_Totals': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 21), 'Lifted_Index': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 22), 'K_Index': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 23), 'Water_Vapor': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 24), 'Water_Vapor_Direct': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 25), 'Water_Vapor_Low': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 26), 'Water_Vapor_High': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 27), 'Quality_Assurance': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07', 'Output_Parameter:mod07'), (406, 270, 10), 20, 28), 'Quality_Assurance_Infrared': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07', 'Water_Vapor_QA_Bytes:mod07'), (406, 270, 5), 20, 29)}




dat = rxr.open_rasterio('MYD07_L2.A2023051.0945.061.2023051234722.hdf',
                       masked=True)

warnings.simplefilter('ignore')

# Get credentials from here: https://data.laadsdaac.earthdatacloud.nasa.gov/s3credentials
#os.environ['AWS_ACCESS_KEY_ID']  = ''
#os.environ['AWS_SECRET_ACCESS_KEY'] = ''
#os.environ['AWS_SESSION_TOKEN'] = ''





for obj in mybucket.objects.all():
    print(obj)

wr.s3.select_query(
        sql="SELECT * FROM s3object s limit 5",
        path="s3://prod-lads/MYD07_L2/"
)

wr.s3.list_objects('s3://prod-lads/MYD07_L2')

filename = 'MYD07_L2.A2023051.0945.061.2023051234722.hdf'
h=open(filename, 'rb')
bts = h.read(4)
print(bts)

pd.read_hdf(filename)

modis_pre = rxr.open_rasterio(filename,
                              masked=True)

with h5py.File(filename, "r") as f:
    # Print all root level object names (aka keys) 
    # these can be group or dataset names 
    print("Keys: %s" % f.keys())
    # get first object name/key; may or may NOT be a group
    a_group_key = list(f.keys())[0]

    # get the object type for a_group_key: usually group or dataset
    print(type(f[a_group_key])) 

    # If a_group_key is a group name, 
    # this gets the object names in the group and returns as a list
    data = list(f[a_group_key])

    # If a_group_key is a dataset name, 
    # this gets the dataset values and returns as a list
    data = list(f[a_group_key])
    # preferred methods to get dataset values:
    ds_obj = f[a_group_key]      # returns as a h5py dataset object
    ds_arr = f[a_group_key][()]  # returns as a numpy array

dat10 = pd.read_hdf('s3://prod-lads/MYD07_L2/MYD07_L2.A2023051.0945.061.2023051234722.hdf')