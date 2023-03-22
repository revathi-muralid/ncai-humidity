
# Created on: 3/21/23 by RM
# Last updated: 3/22/23 by RM

# Import libraries
import polars as pl
import awswrangler as wr
import folium
from folium import plugins
from folium.features import DivIcon
import reverse_geocoder as rgcr
import matplotlib as mpl
import matplotlib.pyplot as plt
import missingno as msno
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

######################  READ IN HADLEY ISD DATA ###################### 

hadley = pd.read_parquet('s3://ncai-humidity/had-isd/Hadley_ISD_ALL_clean.parquet')

had0 = hadley[hadley.time.dt.year==2000]
had10 = hadley[hadley.time.dt.year==2010]
had21 = hadley[hadley.time.dt.year==2021]

hadley=hadley.reset_index()
had0=had0.reset_index()
had10=had10.reset_index()
had21=had21.reset_index()

###################### (1) - Summarize completeness of data ######################

msno.matrix(had21[["temperatures","dewpoints","elevation","slp","stnlp","windspeeds"]])
plt.show()

#################### (2) Create state-level time series plots ####################

viridis = mpl.colormaps['tab20'].resampled(11)

statePalette = {'Alabama': viridis.colors[1],
                'Arkansas': viridis.colors[2],
                'Florida': viridis.colors[3],
                'Georgia': viridis.colors[4],
                'Kentucky': viridis.colors[5],
                'Louisiana': viridis.colors[6],
                'Mississippi': viridis.colors[7],
                'North Carolina': viridis.colors[8],
                'South Carolina': viridis.colors[9],
                'Tennessee': viridis.colors[10],
                'Virginia': viridis.colors[0]}

fig, axes = plt.subplots(3,4, figsize=(12,5))
for (state, group), ax in zip(hadley.groupby('state'), axes.flatten()):
    mycolor=statePalette[state]
    group.plot(x='time', y='temperatures', kind='line', ax=ax, title=state,
              color=mycolor,legend=False)
    ax.set_xlabel('')
    fig.tight_layout()

fig.suptitle('Hadley Time Series of Hourly Air Temperatures in 11 SE States')
fig.subplots_adjust(top=0.88)
fig.delaxes(axes[2][3])

#################### (3) Create annual time series plots ####################

d10 = had0[had0["station_id"]=='720277-63843']

d10["month"] = d10['time'].dt.month
d10['day'] = d10['time'].dt.day

d10 = d10.groupby(["month","day","station_id"], as_index=False).mean("temperatures")

d10 = d10.reset_index()

d10_temp=d10[["time","temperatures",
        "dewpoints"]]

d10_temp.plot("time", figsize=(15, 6))
plt.show()

### Entire time period

d10 = hadley[hadley["station_id"]=='720277-63843']

d10["date"] = d10['time'].dt.date

d10 = d10.groupby(["date"], as_index=False).mean(["temperatures","dewpoints"])

d10 = d10.reset_index()

d10_temp=d10[["date","temperatures",
        "dewpoints"]]

d10_temp.plot("date", figsize=(15, 6))
plt.title("Hadley ISD - Daily mean and air dew point temperatures for 2000-2022 for station ID 720277")
plt.show()

# See if you can subset

d4_sub = ds_masked.where(ds_masked.station_id=='744658-53889',drop=True)

d4_sub = data.where(data.station_id=='744658-53889',drop=True)

d4_masked.temperatures.plot()
d4_masked.dewpoints.plot()
d4_masked.windspeeds.plot()
d4_masked.stnlp.plot()
plt.title("Time series for station ID 747808-63803")
plt.xlabel("Time of Measurement (Year)")
plt.ylabel("Temperature (deg. C)")

#################### (4) Output summary stats ####################

summ = hadley.describe()
summ.to_csv('hadley_stats.csv')

### Aggregated stats

hadley["month"] = hadley['time'].dt.month
hadley['day'] = hadley['time'].dt.day

hadley = hadley.groupby(["month","day","station_id"], as_index=False).mean("temperatures")

hadley = hadley.reset_index()

summ = hadley.describe()
summ.to_csv('hadley_daily_stats.csv')
