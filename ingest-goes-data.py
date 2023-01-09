### Created on: 12/1/22 by RM
### Last updated: 12/1/22 by RM

# pip install s3fs
import pdbufr
from eccodes import *

import pybufrkit
from gribapi import *

import s3fs
import boto3

from pybufrkit.decoder import Decoder

INPUT = 'GOES08.RMD.J2000002.T0200Z'


def example():
    # open bufr file
    f = open(INPUT, 'rb')
 
    cnt = 0
 
    # loop for the messages in the file
    while 1:
        # get handle for message
        bufr = codes_bufr_new_from_file(f)
        if bufr is None:
            break
 
        print("message: %s" % cnt)
 
        # we need to instruct ecCodes to expand all the descriptors
        # i.e. unpack the data values
        codes_set(bufr, 'unpack', 1)
 
        # ---------------------------------------------
        # get values for keys holding a single value
        # ---------------------------------------------
        # Native type integer
        key = 'blockNumber'
 
        try:
            print('  %s: %s' % (key, codes_get(bufr, key)))
        except CodesInternalError as err:
            print('Error with key="%s" : %s' % (key, err.msg))
 
        # Native type integer
        key = 'stationNumber'
        try:
            print('  %s: %s' % (key, codes_get(bufr, key)))
        except CodesInternalError as err:
            print('Error with key="%s" : %s' % (key, err.msg))
 
        # Native type float
        key = 'airTemperatureAt2M'
        try:
            print('  %s: %s' % (key, codes_get(bufr, key)))
        except CodesInternalError as err:
            print('Error with key="%s" : %s' % (key, err.msg))
 
        # Native type string
        key = 'typicalDate'
        try:
            print('  %s: %s' % (key, codes_get(bufr, key)))
        except CodesInternalError as err:
            print('Error with key="%s" : %s' % (key, err.msg))
 
        # --------------------------------
        # get values for an array
        # --------------------------------
        # Native type integer
        key = 'bufrdcExpandedDescriptors'
 
        # get size
        num = codes_get_size(bufr, key)
        print('  size of %s is: %s' % (key, num))
 
        # get values
        values = codes_get_array(bufr, key)
        for i in range(len(values)):
            print("   %d %06d" % (i + 1, values[i]))
 
        # Native type float
        key = 'numericValues'
 
        # get size
        num = codes_get_size(bufr, key)
        print('  size of %s is: %s' % (key, num))
 
        # get values
        values = codes_get_array(bufr, key)
        for i in range(len(values)):
            print("   %d %.10e" % (i + 1, values[i]))
 
        cnt += 1
 
        # delete handle
        codes_release(bufr)
 
    # close the file
    f.close()

decoder = Decoder()
with open('GOES08.RMD.J2000002.T0200Z', 'rb') as ins:
    bufr_message = decoder.process(ins.read())

bucket = "ncai-humidity"
file_name = "GOES/raw/GOES08.RMD.J2000001.T1200Z"

s3 = boto3.client('s3') 
# 's3' is a key word. create connection to S3 using default config and all buckets within S3

obj = s3.get_object(Bucket= bucket, Key= file_name) 
# get object and file (key) from bucket




gribapi.ENC = "unicode-escape"
initial_df = pdbufr.read_bufr("GOES08.RMD.J2000002.T1200Z",columns="") # 'Body' is a key word

s3.download_file('ncai-humidity', 'GOES/raw/GOES08.RMD.J2000001.T0100Z','testing.bufr')

df_all = pdbufr.read_bufr('temp.bufr', columns=('stationNumber', 'latitude', 'longitude'))

https://www.ncei.noaa.gov/pub/has/HAS012334464/GOES08.RMD.J2000001.T0100Z

https://

wget --no-check-certificate --no-proxy 'http://ncai-humidity.s3.us-east-1.amazonaws.com/GOES/raw/GOES08.RMD.J2000001.T0100Z?response-content-disposition=attachment&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEO7%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMiJIMEYCIQCo5OLR3UvSwrn7j2uf6DOXC8W6gK8xkeGSX0ZyUWEOsAIhAOJrZry70EBiWVNdnnG8rycKWr3Yzeb1eWDS6t7pkzdUKucDCBcQABoMNjY2ODUyOTMzMzIzIgyj4wra2tyVq1ABfV0qxAO4pvRhNfvYA76DmXpS%2BM6kClojKtURKEafUyIcLQi5ubkql24eyfajcK6m5AQ96See2JTkjcavUJnNq3mxFHvc5ntU%2F0oRgKWRmxG3FGBAv401dCxdgaNrVLwWqEAnjoB9GqgDsbdNQbgO63R%2BB3qrTAMng5eTNZKukcgLiSOXiH3M%2B3hjRCkWBZKognpBS%2Ff%2FFijlYRyuQ9%2BtGCDlC632EWG7%2BuSBoTI1IlnbnlG8Afy4BcI1JAZRo5BVEPmgsDpB5ImkiODFTWZ20caKtC1ecCYYGzOIwCTKDPo13iONsFAt4vZ%2Fxra9LW3QPvl3OJUCY4s3x2dvArPlCqUMiclXNZ%2BWw5W1y%2FCYPA5rEMmw3c0%2FQUOKCZZZEihizxpVxcfjbcn5I1akYEq7Kr58ZbOyqyWm9RnhHSRimNfMu1AR1muUgESQjw7kuwlWg%2BDO0FrE16YMZS3yFb1SstaWpMq0r2DR197QN9uBaxzuj0CUW2WZ%2BYzIoBNhlEOSXhN9ePRvmDbVXU%2BJAlBkvssRJNWu3rtZ38NatWRSsFtgx8Fmcqo%2BwNZ8puezeNt25XtLzcLOoa%2Fyfg0ag7dyOZsGFXXhSjRIcDDMlL2cBjqTAm6rmM%2Fhg%2F%2FbMSkGc5gJisdt0LRl0qs4B3lS%2FPAmCAwXrB8a0SGzlEDc3dahAD8EiA%2FYObYcSWhQFnKbbxqvImAUIVTjZt2iKadWYSh15CQoAqHb17XSmwc%2FEg5APxoH9o%2F0NuGjKqQJRKgcyGwaGgX5AU8cw%2FgSu4udd9735ZFmIpYJRYD4IlibmnB%2FvVIiAYK6STLxqxC1lOojlFvvzHDvJX599OURqbCQxis9QUuR4ligsR172Xu5TJFPhWQMnmM1BADAXQnlpzIO7ybquJf6FdcQGDpK%2FEAoaM%2BJr8%2FwaIw0AF6n5ZZDpV1VeJpY%2F9jnksOuA9C1LrQHHkzY2Z4E%2BYXwsSsEwMtNf%2B1a7bWokTf%2F&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20221206T144746Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAZWQ4IH3FYL4IH56C%2F20221206%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=1abd18ab0ff1c160b86f12a8a2dc791651ef329f71e272f3171205b22ab63bfd'