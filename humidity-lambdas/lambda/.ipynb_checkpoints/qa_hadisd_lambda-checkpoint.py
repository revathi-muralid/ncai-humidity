######################

# Adopted from a template from a previous lambda created by DW

######

import os
import json
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

s3 = s3fs.S3FileSystem(anon=False)
s3path = 's3://ncai-humidity/had-isd/hourly/pq/*'
remote_files = s3.glob(s3path)

s3_df = pd.DataFrame(remote_files)
s3_df=s3_df.rename(columns={0:"fname"})
s3_df['id']=s3_df['fname'].str[32:-8]

stations = stations.rename(columns={0:"id"})
stations = stations.merge(s3_df,on="id",how="left")

qa = stations[pd.isna(stations['fname'])==True]

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

    stn_id = stations.iloc[myrow]['id']
    url = (
        "https://www.metoffice.gov.uk/hadobs/hadisd/v330_2022f/data/hadisd.3.3.0.2022f_19310101-20230101_"
        + str(stn_id)
        + ".nc.gz"
    )
    new = "/tmp/" + str(stn_id) + ".nc.gz"

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
        temps[temps==ds.variables["temperatures"].flagged_value] = np.nan
        temps[temps==ds.variables["temperatures"].missing_value] = np.nan
        
        dewpts = ds.variables["dewpoints"][:].data
        dewpts[dewpts==ds.variables["dewpoints"].flagged_value] = np.nan
        dewpts[dewpts==ds.variables["dewpoints"].missing_value] = np.nan
        
        slp = ds.variables["slp"][:].data
        slp[slp==ds.variables["slp"].flagged_value] = np.nan
        slp[slp==ds.variables["slp"].missing_value] = np.nan
        
        stnlp = ds.variables["stnlp"][:].data
        stnlp[stnlp==ds.variables["stnlp"].flagged_value] = np.nan
        stnlp[stnlp==ds.variables["stnlp"].missing_value] = np.nan
        
        windspeeds = ds.variables["windspeeds"][:].data
        windspeeds[windspeeds==ds.variables["windspeeds"].flagged_value] = np.nan
        windspeeds[windspeeds==ds.variables["windspeeds"].missing_value] = np.nan
        
        qc_flags = ds.variables["quality_control_flags"][:,:].data.astype(int)
        
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

    
    df.to_parquet("s3://ncai-humidity/had-isd/hourly/pq/" + stn_id + ".parquet")
    
    os.remove(infile)
    os.remove(new)

[getHadISDData(q) for q in qa.index]
