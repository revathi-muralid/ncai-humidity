######################

# Adopted from a template from a previous lambda created by DW

######

import os
import json
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
import requests
import urllib
import netCDF4
from netCDF4 import Dataset, num2date
import gzip
import tempfile
import shutil
import xarray as xr
import zarr
import s3fs

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get bounding box

# NW corner: 36.4996,-94.617919
# NE corner: 39.466012,-75.242266
# SW corner: 28.928609,-94.043147
# SE corner: 24.523096,-80.031362
# AR | Arkansas | -94.617919 | 33.004106 | -89.644395 | 36.4996 |
# VA | Virginia | -83.675395 | 36.540738 | -75.242266 | 39.466012 |
# FL | Florida | -87.634938 | 24.523096 | -80.031362 | 31.000888 |
# LA | Louisiana | -94.043147 | 28.928609 | -88.817017 | 33.019457 |

min_lon = -94.617919
min_lat = 24.523096
max_lon = -75.242266
max_lat = 39.466012

# Get US station IDs from HadISD station list
path_to_file = "hadisd_station_info_v330_2022f.txt"

df = pd.read_table(path_to_file, sep="\s+", header=None)
new_df = df[~df[0].str.contains("99999")]
stations = new_df[new_df[0].str[0:1].isin(["7"])]
stations = stations[stations[1] > min_lat]  # 1458
stations = stations[stations[1] < max_lat]  # 740
stations = stations[stations[2] > min_lon]  # 401
stations = stations[stations[2] < max_lon]  # 396

isd_epoch = datetime(1931, 1, 1, 0, 0, tzinfo=timezone.utc)


def getHadISDData(myrow):
    """

    This function download data from the Hadley ISD website for every station in the Southeast
    for the years between 2000 and 2022. This function also selects only the variables relevant
    for analysis. Data are output to an s3 bucket (ncai-humidity/had-isd/hourly).

    On return, this function gives the station ID of the Hadley ISD station data was
    downloaded for.

    Arguments:
        myrow (int) : Row number of station in the list of stations

    Returns:
        stn_id : string

    """
    # myrow = int(myrow)

    stn_id = stations.iloc[myrow][0]
    url = (
        "https://www.metoffice.gov.uk/hadobs/hadisd/v330_2022f/data/hadisd.3.3.0.2022f_19310101-20230101_"
        + str(stn_id)
        + ".nc.gz"
    )
    new = "/tmp/" + str(stn_id) + ".nc.gz"

    # ADdED LINE OF CODE
    start = datetime.now()
    urllib.request.urlretrieve(url, new)
    old = "/tmp/" + str(stn_id) + ".nc"

    with gzip.open(new, "rb") as f_in:
        with open(old, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    infile = old

    ds = netCDF4.Dataset(infile)

    if (
        (ds.variables["longitude"][:].data > min_lon)
        & (ds.variables["longitude"][:].data < max_lon)
        & (ds.variables["latitude"][:].data > min_lat)
        & (ds.variables["latitude"][:].data < max_lat)
    ) == True:
        times = ds.variables["time"][:].data.astype(int)
        times = [isd_epoch + timedelta(hours=int(t)) for t in times]

        lat = float(ds.variables["latitude"][:].data)
        lon = float(ds.variables["longitude"][:].data)

        temps = ds.variables["temperatures"][:].data
        dewpts = ds.variables["dewpoints"][:].data
        slp = ds.variables["slp"][:].data
        stnlp = ds.variables["stnlp"][:].data
        windspeeds = ds.variables["windspeeds"][:].data
        qc_flags = ds.variables["quality_control_flags"][:].data.astype(int)
        # qc_flags2=[''.join(map(str, qc_flags[i])) for i in qc_flags]
        flg_obs = ds.variables["flagged_obs"][:].data

        df = pd.DataFrame(
            {
                "temp": temps,
                "dewpoint": dewpts,
                "slp": slp,
                "stnlp": stnlp,
                "windspeeds": windspeeds,
            }
        )
        df["time"] = times
        df["stn_id"] = stn_id
        df["lon"] = lon
        df["lat"] = lat

        df = df[
            [
                "stn_id",
                "lon",
                "lat",
                "time",
                "temp",
                "dewpoint",
                "slp",
                "stnlp",
                "windspeeds",
            ]
        ]

    # ds = xr.open_dataset(infile)

    end = datetime.now()

    logger.info("Read in Data in " + str(end - start))

    # ds = ds.where(ds.coords["latitude"] > min_lat, drop=True)
    # ds = ds.where(ds.coords["latitude"] < max_lat, drop=True)
    # ds = ds.where(ds.coords["longitude"] > min_lon, drop=True)
    # ds = ds.where(ds.coords["longitude"] < max_lon, drop=True)
    # ds = ds.sel(time=slice("2000-01-01", "2023-01-01"))

    variables = [
        "station_id",
        "temperatures",
        "dewpoints",
        "slp",
        "stnlp",
        "windspeeds",
        "quality_control_flags",
        "flagged_obs",
        "reporting_stats",
    ]

    # ds = ds[variables]

    # ds.to_zarr(store= s3fs.S3Map(root=f's3://ncai-humidity/had-isd/hourly/'+stn_id+'.zarr', s3=s3 ,check=False))

    try:
        start = datetime.now()
        # ds.to_zarr("s3://ncai-humidity/had-isd/hourly/" + stn_id + ".zarr", mode="w")
        df.to_parquet("s3://ncai-humidity/had-isd/hourly/pq/" + stn_id + ".parquet")
        end = datetime.now()

        logger.info("Wrote in .zarr Data in " + str(end - start))

    except Exception as e:
        logger.critical(e, exc_info=True)

    os.remove(infile)
    os.remove(new)

    return {"statusCode": 200, "body": json.dumps(stn_id)}


def lambda_handler(event, context):
    logger.info("## ENVIRONMENT VARIABLES")
    logger.info(os.environ)
    logger.info("## EVENT")
    logger.info(event)

    # Message
    event_body = json.loads(event["Records"][0]["body"])
    my_row = event_body["myrow"]
    logger.info(f"My Row: {my_row}")

    print(my_row)

    transfer_output = getHadISDData(my_row)

    return transfer_output
