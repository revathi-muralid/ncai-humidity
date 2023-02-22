import xarray
import rioxarray as rxr
import pandas as pd
import os
import awswrangler as wr
import s3fs
import boto3
import warnings
import pyhdf.SD as SD
from pyhdf.SD import SD, SDC

s3 = boto3.client('s3')
mybucket = s3.Bucket('prod-lads')
s3.download_file('prod-lads','MOD07_L2','MOD07_L2.A2000068.2250.061.2017202230318.hdf')
with open('MOD07_L2.A2000068.2250.061.2017202230318.hdf', 'wb') as f:
    s3.download_fileobj('prod-lads', 'MOD07_L2', f)
    
myobjs = mybucket.objects.all()

testpath = 's3://prod-lads/MOD07_L2/MOD07_L2.A2000068.2250.061.2017202230318.hdf'

pd.read_hdf(testpath)

hdf = SD('MYD07_L2.A2023051.0945.061.2023051234722.hdf', SDC.READ)
hdf2 = SD(testpath, SDC.READ)

print(hdf.datasets())
# {'Latitude': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 5, 0), 'Longitude': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 5, 1), 'Scan_Start_Time': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 6, 2), 'Solar_Zenith': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 3), 'Solar_Azimuth': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 4), 'Sensor_Zenith': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 5), 'Sensor_Azimuth': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 6), 'Brightness_Temperature': (('Band_Number:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (12, 406, 270), 22, 7), 'Cloud_Mask': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 20, 8), 'Skin_Temperature': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 9), 'Surface_Pressure': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 10), 'Surface_Elevation': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 11), 'Processing_Flag': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 20, 12), 'Tropopause_Height': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 13), 'Guess_Temperature_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 14), 'Guess_Moisture_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 15), 'Retrieved_Temperature_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 16), 'Retrieved_Moisture_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 17), 'Retrieved_WV_Mixing_Ratio_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 18), 'Retrieved_Height_Profile': (('Pressure_Level:mod07', 'Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (20, 406, 270), 22, 19), 'Total_Ozone': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 20), 'Total_Totals': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 21), 'Lifted_Index': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 22), 'K_Index': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 23), 'Water_Vapor': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 24), 'Water_Vapor_Direct': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 25), 'Water_Vapor_Low': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 26), 'Water_Vapor_High': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07'), (406, 270), 22, 27), 'Quality_Assurance': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07', 'Output_Parameter:mod07'), (406, 270, 10), 20, 28), 'Quality_Assurance_Infrared': (('Cell_Along_Swath:mod07', 'Cell_Across_Swath:mod07', 'Water_Vapor_QA_Bytes:mod07'), (406, 270, 5), 20, 29)}


clouds = hdf.select('Cloud_Mask')
cldat = clouds[:,:]

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