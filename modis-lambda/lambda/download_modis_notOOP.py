######################

# Adopted from a template from a previous lambda created by DW

######

import xarray
import pandas as pd
import os
import boto3
import requests
import warnings
import logging
import pyhdf.SD as SD
from pyhdf.SD import SD, SDC, SDAttr
from numpy import *
import numpy as np
from datetime import datetime
from datetime import timedelta
from itertools import product

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get list of MOD07_L2 filenames
filenames = pd.read_csv('../../LAADS_fnames_2000_22.csv')
myfiles = list(filenames['filename'])
s3west = boto3.resource('s3',region_name='us-west-1',aws_access_key_id=os.environ['NSAND_ACCESS'], aws_secret_access_key=os.environ['NSAND_SECRET'],aws_session_token=os.environ['NSAND_SESSION'])

s3 = boto3.resource('s3',region_name='us-east-1',aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],aws_session_token=os.environ['AWS_SESSION_TOKEN'])

def getMOD07L2Data(myind):
    """

    This function downloads data from the MOD07_L2 AWS bucket for every station in the Southeast
    for the years between 2000 and 2022. This function also selects only the variables relevant
    for analysis. Data are output to an s3 bucket (ncai-humidity/MODIS/MOD07_L2).

    On return, this function gives the name of the file that was processed.

    Arguments:
        myind (int) : File index in the list of files

    Returns:
        f1 : string

    """
    # myrow = int(myrow)

    f1 = myfiles[myind]
    
    s3west.meta.client.download_file('prod-lads', 'MOD07_L2/%s'%f1, '/tmp/%s'%f1)

    # ADdED LINE OF CODE
    start = datetime.now()
    print(start)
    hdf = SD('/tmp/%s'%f1, SDC.READ)

    # Print dataset names

    ds_dic = hdf.datasets()
    my_dic = dict((k, ds_dic[k]) for k in ('Latitude','Longitude','Scan_Start_Time',
                                           'Cloud_Mask','Surface_Pressure',
                                           'Surface_Elevation','Processing_Flag',
                                           'Retrieved_Temperature_Profile',
                                           'Retrieved_Moisture_Profile',
                                           'Water_Vapor','Water_Vapor_Low',
                                           'Water_Vapor_High'))
    
    # Deal with 'Quality_Assurance' variable separately

    vars = {}
    for idx,sds in enumerate(my_dic.keys()):
        x = hdf.select(sds)
        xs = x.get()
        xs = pd.DataFrame(xs.flatten())
        vars[sds] = xs
        print (idx,sds)

    end = datetime.now()

    logger.info("Read in Data in " + str(end - start))

    qa = hdf.select('Quality_Assurance')
    qas = qa.get()
    
    qa_new=[]
    for i,j in product(range(qas.shape[0]),range(qas.shape[1])):
        temp = " ".join([str(item) for item in qas[i][j]])
        qa_new = np.append(qa_new, temp)

    qa_fordf = pd.DataFrame(qa_new.flatten())
    vars['Quality_Assurance']=qa_fordf

    dfind = [qa_fordf[0][q][0:2]=='51' for q in range(qa_fordf.shape[0])] 

    qa_fordf = qa_fordf[dfind] # 25964/109620
    
    # Set NaNs
    vars['Retrieved_Temperature_Profile'][vars['Retrieved_Temperature_Profile']==-32768] = np.nan
    vars['Retrieved_Moisture_Profile'][vars['Retrieved_Moisture_Profile']==-32768] = np.nan
    vars['Surface_Elevation'][vars['Surface_Elevation']==-32768] = np.nan
    vars['Surface_Pressure'][vars['Surface_Pressure']==-32768] = np.nan
    vars['Water_Vapor'][vars['Water_Vapor']==-9999] = np.nan
    vars['Water_Vapor_Low'][vars['Water_Vapor_Low']==-9999] = np.nan
    vars['Water_Vapor_High'][vars['Water_Vapor_High']==-9999] = np.nan

    # Scale and adjust
    surf_press_sf = 0.1000000014901161
    vars['Surface_Pressure'] = vars['Surface_Pressure']*surf_press_sf

    # same sf and ao for ret temp and ret moist
    ret_temp_sf = 0.009999999776482582 
    ret_temp_ao = -15000
    vars['Retrieved_Temperature_Profile'] = ret_temp_sf*(vars['Retrieved_Temperature_Profile']-ret_temp_ao)
    vars['Retrieved_Moisture_Profile'] = ret_temp_sf*(vars['Retrieved_Moisture_Profile']-ret_temp_ao)

    wat_vap_sf = 0.001000000047497451
    vars['Water_Vapor'] = wat_vap_sf*vars['Water_Vapor']
    vars['Water_Vapor_Low'] = wat_vap_sf*vars['Water_Vapor_Low']
    vars['Water_Vapor_High'] = wat_vap_sf*vars['Water_Vapor_High']
    
    nrows = qas.shape[0]*qas.shape[1]
    
    ### Only keep rows for lowest pressure level

    vars['Retrieved_Temperature_Profile'] = vars['Retrieved_Temperature_Profile'][nrows*18:nrows*19]
    vars['Retrieved_Moisture_Profile'] = vars['Retrieved_Moisture_Profile'][nrows*18:nrows*19]

    ### Only keep rows that have good data quality

    fvars = {}
    for idx,sds in enumerate(my_dic.keys()):
        xs = vars[sds]
        xs = xs[dfind]
        fvars[sds] = xs
        print (idx,sds)

    fvars['Quality_Assurance'] = qa_fordf
    
    # Scan_Start_Time is given in seconds since 1993

    fvars['Scan_Start_Time']=fvars['Scan_Start_Time'].reset_index()

    orig_date = datetime.strptime('01-01-1993', '%d-%m-%Y')
    mytimes = [orig_date + timedelta(seconds=fvars['Scan_Start_Time'][0][x]) for x in range(len(fvars['Scan_Start_Time']))]

    fvars['Scan_Start_Time'] = pd.DataFrame(mytimes)

    frames = fvars['Latitude'].reset_index(), fvars['Longitude'].reset_index(), fvars['Scan_Start_Time'].reset_index(), fvars['Cloud_Mask'].reset_index(), fvars['Surface_Pressure'].reset_index(), fvars['Surface_Elevation'].reset_index(), fvars['Processing_Flag'].reset_index(), fvars['Retrieved_Temperature_Profile'].reset_index(), fvars['Retrieved_Moisture_Profile'].reset_index(), fvars['Water_Vapor'].reset_index(), fvars['Water_Vapor_Low'].reset_index(), fvars['Water_Vapor_High'].reset_index(),fvars['Quality_Assurance'].reset_index()

    df=pd.concat(frames, axis=1, ignore_index=True, verify_integrity=True)
    df=df[list(range(1,27,2))]

    mynames = list(my_dic.keys())
    mynames.append('QA')
    df.columns=mynames
    
    fname = f1.rsplit(".",1)[0]
    
    try:
        start = datetime.now()
        df.to_parquet("s3://ncai-humidity/MODIS/MOD07_L2/" + fname + ".parquet")
        end = datetime.now()

        logger.info("Wrote .parquet file in " + str(end - start))

    except Exception as e:
        logger.critical(e, exc_info=True)

    os.remove('/tmp/%s'%f1)

    return {"statusCode": 200, "body": json.dumps(f1)}

test = pd.read_parquet('s3://ncai-humidity/MODIS/MOD07_L2/MOD07_L2.A2000057.1810.061.2017202185019.parquet')

def lambda_handler(event, context):
    logger.info("## ENVIRONMENT VARIABLES")
    logger.info(os.environ)
    logger.info("## EVENT")
    logger.info(event)

    # Message
    event_body = json.loads(event["Records"][0]["body"])
    my_ind = event_body["myind"]
    logger.info(f"My Index: {my_ind}")

    print(my_ind)

    transfer_output = getMOD07L2Data(my_ind)

    return transfer_output
