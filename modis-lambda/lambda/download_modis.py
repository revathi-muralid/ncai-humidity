######################

# Adopted from a template from a previous lambda created by DW

######

import pandas as pd
import os
from modiswrangler import satDat
import boto3
from datetime import datetime
import logging
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get list of MOD07_L2 filenames
filenames = pd.read_csv('LAADS_fnames_2000_22.csv')
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
    
    d1 = satDat(f1)
    
    start = datetime.now()
    d2 = d1.read()
    end = datetime.now()
    logger.info("Read in Data in " + str(end - start))
    
    
    try:
        start = datetime.now()
        d1.df.to_parquet("s3://ncai-humidity/MODIS/MOD07_L2/" + d1.outname + ".parquet")
        end = datetime.now()

        logger.info("Wrote .parquet file in " + str(end - start))

    except Exception as e:
        logger.critical(e, exc_info=True)

    os.remove('/tmp/%s'%f1)

    return {"statusCode": 200, "body": json.dumps(f1)}

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
