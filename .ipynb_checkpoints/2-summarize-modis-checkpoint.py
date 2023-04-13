
# Created on: 4/11/23 by RM
# Last updated: 4/11/23 by RM

import pandas as pd
import os
import awswrangler as wr
import s3fs
import boto3
from numpy import *
import numpy as np
from datetime import datetime
from datetime import timedelta
import json
import folium
from folium import plugins
from folium.features import DivIcon

fname = 'MYD07_L2.A2022001.1645.061.2022002160950'
test = pd.read_parquet("s3://ncai-humidity/MODIS/MYD07_L2/" + fname + ".parquet")

test2 = pd.read_parquet("s3://ncai-humidity/had-isd/Hadley_ISD_ALL_clean.parquet")

test3 = pd.read_parquet("s3://ncai-humidity/MODIS/MOD07_L2/MOD07_L2.A2000056.1555.061.2017202184311.parquet")

test=test.reset_index(drop=True, inplace=True)

with open('gz_2010_us_040_00_500k.json') as f:
    ncArea = json.load(f)

#initialize the map around northeast corner of alabama

ncMap = folium.Map(location=[32.318230,-86.902298], tiles='Stamen Toner', zoom_start=6)

#for each row in the dataset, plot the corresponding latitude and longitude on the map
for i,row in test.iterrows():
    folium.CircleMarker((test['Latitude'],test['Longitude']), radius=3, weight=2, color='red', fill_color='red', fill_opacity=.5).add_to(ncMap)
    folium.map.Marker((test['Latitude'],test['Longitude']),
                      icon=DivIcon(
                          icon_size=(10,10),
                          icon_anchor=(5,14),
                          html=f'<div style="font-size: 14pt">%s</div>' % str(i),
                      )
                     ).add_to(ncMap)

#add the boundaries of the US to the map
folium.GeoJson(ncArea).add_to(ncMap)

ncMap