### Created on: 12/1/22 by RM
### Last updated: 12/1/22 by RM

# conda install -c conda-forge python-eccodes pandas
# pip install pdbufr
import pdbufr
import s3fs
import boto3

s3 = boto3.client('s3')
s3.download_file('ncai-humidity', 'GOES/raw/','GOES08.RMD.J2000001.T0100Z')

df_all = pdbufr.read_bufr('temp.bufr', columns=('stationNumber', 'latitude', 'longitude'))

https://www.ncei.noaa.gov/pub/has/HAS012334464/GOES08.RMD.J2000001.T0100Z
