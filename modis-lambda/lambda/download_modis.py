######################

# Adopted from a template from a previous lambda created by DW

######

import pandas as pd
import os
from modiswrangler import satDat
import awswrangler as wr
import boto3
from datetime import datetime
import logging
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# stream = logging.FileHandler()
# stream.setLevel(logging.DEBUG)
# logger.addHandler(stream)

# Get list of MOD07_L2 filenames
filenames = pd.read_csv("LAADS_fnames_2000_22.csv")
myfiles = list(filenames["filename"])

s3west_sesh = boto3.Session(
    region_name="us-west-2",
    aws_access_key_id=os.environ["NSAND_ACCESS"],
    aws_secret_access_key=os.environ["NSAND_SECRET"],
    aws_session_token=os.environ["NSAND_SESSION"],
)

s3east_sesh = boto3.Session(
    region_name="us-east-1",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID_"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY_"],
    aws_session_token=os.environ["AWS_SESSION_TOKEN_"],
)

s3west = s3west_sesh.resource("s3")
s3east = s3east_sesh.resource("s3")


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
    myind = int(myind)

    f1 = myfiles[myind]

    # s3west.meta.client.download_file('prod-lads', 'MOD07_L2/%s'%f1, '/tmp/%s'%f1)
    wr.s3.download(
        path=f"s3://prod-lads/MYD07_L2/{f1}",
        local_file=f"/tmp/{f1}",
        boto3_session=s3west_sesh,
    )

    d1 = satDat(f1)

    invars = (
        "Latitude",
        "Longitude",
        "Scan_Start_Time",
        "Cloud_Mask",
        "Surface_Pressure",
        "Surface_Elevation",
        "Processing_Flag",
        "Retrieved_Temperature_Profile",
        "Retrieved_Moisture_Profile",
        "Water_Vapor",
        "Water_Vapor_Low",
        "Water_Vapor_High",
    )

    myqavar = "Quality_Assurance"

    start = datetime.now()
    d1.read(invars, myqavar)
    end = datetime.now()
    logger.info("Read in Data in " + str(end - start))

    if d1.qa_fordf.shape[0] != 0:
        # Treat NaNs
        # Then scale and adjust

        surf_press_sf = 0.1000000014901161
        surf_press_ao = 0

        ret_temp_sf = 0.009999999776482582
        ret_temp_ao = -15000

        wat_vap_sf = 0.001000000047497451
        wat_vap_ao = 0

        # Same scaling factors and add_offsets for the next 2 vars

        d1.mask_nans("Retrieved_Temperature_Profile")
        d1.scale_and_adjust("Retrieved_Temperature_Profile", ret_temp_sf, ret_temp_ao)

        d1.mask_nans("Retrieved_Moisture_Profile")
        d1.scale_and_adjust("Retrieved_Moisture_Profile", ret_temp_sf, ret_temp_ao)

        d1.mask_nans("Surface_Elevation")

        d1.mask_nans("Surface_Pressure")
        d1.scale_and_adjust("Surface_Pressure", surf_press_sf, surf_press_ao)

        # Same scaling factors and add_offsets for the next 3 vars

        d1.mask_nans("Water_Vapor")
        d1.scale_and_adjust("Water_Vapor", wat_vap_sf, wat_vap_ao)

        d1.mask_nans("Water_Vapor_Low")
        d1.scale_and_adjust("Water_Vapor_Low", wat_vap_sf, wat_vap_ao)

        d1.mask_nans("Water_Vapor_High")
        d1.scale_and_adjust("Water_Vapor_High", wat_vap_sf, wat_vap_ao)

        # Only keep rows for lowest pressure level

        d1.qc_data("Retrieved_Temperature_Profile", "Retrieved_Moisture_Profile")

        # Clean time variable

        d1.format_time("Scan_Start_Time")

        d1.make_df()

        try:
            start = datetime.now()
            wr.s3.to_parquet(
                d1.df,
                path="s3://ncai-humidity/MODIS/MYD07_L2/" + d1.outname + ".parquet",
                boto3_session=s3east_sesh,
            )
            print("\nWoohoo! File " + str(myind) + " output to S3 successfully!\n")
            end = datetime.now()
            logger.info("Wrote .parquet file in " + str(end - start))

        except Exception as e:
            logger.critical(e, exc_info=True)

    else:
        print("\nFile " + str(myind) + " has invalid data!\n")
        logger.info("### File " + str(myind) + " has invalid data!")
        pass

    os.remove("/tmp/%s" % f1)

    return {"statusCode": 200, "body": json.dumps(f1)}


def lambda_handler(event, context):
    logger.info("## ENVIRONMENT VARIABLES")
    logger.info(os.environ)
    logger.info("## EVENT")
    logger.info(event)

    # Message
    event_body = json.loads(event["Records"][0]["body"])
    logger.info("## WHAT ON EARTH IS OUTPUTTING TO EVENT_BODY!!!!!")
    logger.info(event_body)
    my_ind = event_body["myind"]
    # my_ind = int(my_ind)
    logger.info(f"My Index: {my_ind}")

    print(my_ind)

    transfer_output = getMOD07L2Data(my_ind)

    return transfer_output


### Uncomment below block of code for testing

# def getMOD07L2Data(myind):
#     """

#     This function downloads data from the MOD07_L2 AWS bucket for every station in the Southeast
#     for the years between 2000 and 2022. This function also selects only the variables relevant
#     for analysis. Data are output to an s3 bucket (ncai-humidity/MODIS/MOD07_L2).

#     On return, this function gives the name of the file that was processed.

#     Arguments:
#         myind (int) : File index in the list of files

#     Returns:
#         f1 : string

#     """
#     myind = int(myind)

#     f1 = myfiles[myind]

#     s3west.meta.client.download_file('prod-lads', 'MOD07_L2/%s'%f1, '/tmp/%s'%f1)

#     d1 = satDat(f1)

#     invars = ('Latitude','Longitude','Scan_Start_Time','Cloud_Mask','Surface_Pressure','Surface_Elevation','Processing_Flag','Retrieved_Temperature_Profile','Retrieved_Moisture_Profile','Water_Vapor','Water_Vapor_Low','Water_Vapor_High')

#     myqavar = 'Quality_Assurance'

#     d1.read(invars,myqavar)

#     if d1.qa_fordf.shape[0]!=0:

#         # Treat NaNs
#         # Then scale and adjust

#         surf_press_sf = 0.1000000014901161
#         surf_press_ao=0

#         ret_temp_sf = 0.009999999776482582
#         ret_temp_ao = -15000

#         wat_vap_sf = 0.001000000047497451
#         wat_vap_ao = 0

#         # Same scaling factors and add_offsets for the next 2 vars

#         d1.mask_nans('Retrieved_Temperature_Profile')
#         d1.scale_and_adjust('Retrieved_Temperature_Profile',ret_temp_sf,ret_temp_ao)

#         d1.mask_nans('Retrieved_Moisture_Profile')
#         d1.scale_and_adjust('Retrieved_Moisture_Profile',ret_temp_sf,ret_temp_ao)

#         d1.mask_nans('Surface_Elevation')

#         d1.mask_nans('Surface_Pressure')
#         d1.scale_and_adjust('Surface_Pressure',surf_press_sf,surf_press_ao)

#         # Same scaling factors and add_offsets for the next 3 vars

#         d1.mask_nans('Water_Vapor')
#         d1.scale_and_adjust('Water_Vapor',wat_vap_sf,wat_vap_ao)

#         d1.mask_nans('Water_Vapor_Low')
#         d1.scale_and_adjust('Water_Vapor_Low',wat_vap_sf,wat_vap_ao)

#         d1.mask_nans('Water_Vapor_High')
#         d1.scale_and_adjust('Water_Vapor_High',wat_vap_sf,wat_vap_ao)

#         # Only keep rows for lowest pressure level

#         d1.qc_data('Retrieved_Temperature_Profile', 'Retrieved_Moisture_Profile')

#         # Clean time variable

#         d1.format_time("Scan_Start_Time")

#         d1.make_df()

#         wr.s3.to_parquet(d1.df, path="s3://ncai-humidity/MODIS/MOD07_L2/" + d1.outname + ".parquet")

#         print("\nWoohoo! File "+str(myind)+" output to S3 successfully!\n")

#     else:

#         print("\nFile "+str(myind)+" has invalid data!\n")
#         pass

#     os.remove('/tmp/%s'%f1)

# start=datetime.now()
# #getMOD07L2Data(list(range(302,306)))
# for i in range(200,300):
#     getMOD07L2Data(i)
# end=datetime.now()
# print(end-start)
# # Took about 11 minutes for 80 files
