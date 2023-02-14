# Created on: 1/18/22 by RM
# Last updated: 1/31/23 by RM
# Purpose: To summarize and explore NOAA Integrated Surface Database (ISD) in situ humidity data

# ISD consists of global hourly observations compiled from an array of different sources
# The data are stored in the S3 bucket s3://noaa-isd-pds in fixed with text file format

# Import libraries
import awswrangler as wr
import polars as pl
import pyarrow.dataset as ds
import boto3
import us
import s3fs
from functools import partial
import pandas as pd
import metpy
import math
from metpy.calc import relative_humidity_from_dewpoint
from metpy.units import units
import numpy.ma as ma
import numpy as np
import scipy as sp
from datetime import datetime as dt
import copy
import qc_utils as utils

myyr = 2000
mystate= 'AL'

# DONE: AL, AR, FL, GA, KY, LA, MS, NC, SC, TN, 
# TO DO: VA

# Load years of interest
study_years = list(range(myyr,myyr+1))

### 2000
subvars2000 = ["('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')","('wind', 'speed')","('wind', 'speed QC')","('air temperature', 'temperature')","('air temperature', 'temperature QC')","('dew point', 'temperature')","('dew point', 'temperature QC')","('sea level pressure', 'pressure')","('sea level pressure', 'pressure QC')","('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')","('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')"]

# Scan in station data
queries = []
study_years = list(range(2000,2023))
for year in study_years:
    q = (
        ds.dataset("s3://ncai-humidity/isd/raw/"+str(mystate)+"-"+str(year)+".parquet", partitioning='hive')
    )
    queries.append(q)
    
# QC flags for inclusion
include_qc = ["5","1","9","A","4","0"]

df = (
    pl.from_arrow(
        ds.dataset(queries)
        .scanner(
            columns = subvars2000#,
            )
        .to_table()
     )
    .filter((pl.col("('Control', 'USAF')")!="999999") & (pl.col("('Control', 'WBAN')")!="999999"))
    .with_columns(pl.col("('Control', 'datetime')").dt.strftime("%Y-%m-%d").alias("date"))
    # .with_column(pl.col("('Control', 'datetime')").dt.month)
    # .with_column(pl.col("('Control', 'datetime')").dt.year)
    .sort(pl.col("('Control', 'datetime')"))   
)

df2 = df.to_pandas()
df2.count()

### QC FILTERING COUNTS FOR 2010
# Filtering out records with both IDs == 999999 results in:
# 1757917 --> 1441510 records (~300k records dropped)
# 'Relative-Humidity-Raw' variables: 3225 --> 3225
# 'Relative-Humidity-Temperature' variables: 315276 --> 0
# 'Hourly-RH-Temperature' variables: 26273 --> 0
###

df_forjoin = df.select(["date","('Control', 'datetime')","('Control', 'USAF')","('Control', 'WBAN')","('Control', 'latitude')","('Control', 'longitude')","('Control', 'elevation')"])

df_forjoin = df_forjoin.unique(subset=["date", "('Control', 'USAF')","('Control', 'WBAN')"])

### INCLUDE FOR ALL YEARS

isd_airtemp = (
    df
    .filter(pl.col("('air temperature', 'temperature QC')").is_in(include_qc))
    #.groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    # .agg([
    #     pl.col(["('air temperature', 'temperature')"]).mean().alias('Mean Air Temp.')
    # ])
)

def sc(station, variable_list, flag_col, start, end, logfile, diagnostics = False, plots = False, doMonth = False):
    '''
    Spike Check, looks for spikes up to 3 observations long, using thresholds
    calculated from the data itself.
    :param MetVar station: the station object
    :param list variable_list: list of observational variables to process
    :param list flag_col: the columns to set on the QC flag array
    :param datetime start: dataset start time
    :param datetime end: dataset end time
    :param file logfile: logfile to store outputs
    :param bool plots: do plots
    :param bool doMonth: account for incomplete months
    :returns:    
    '''
    print "refactor"
    
    for v, variable in enumerate(variable_list):

        flags = station.qc_flags[:, flag_col[v]]

        st_var = getattr(station, variable)
    
        # if incomplete year, mask all obs for the incomplete bit
        all_filtered = utils.apply_filter_flags(st_var, doMonth = doMonth, start = start, end = end)
      
        reporting_resolution = utils.reporting_accuracy(utils.apply_filter_flags(st_var))
        # to match IDL system - should never be called as would mean no data
        if reporting_resolution == -1:
            reporting_resolution = 1 

        month_ranges = utils.month_starts_in_pairs(start, end)
        month_ranges = month_ranges.reshape(-1,12,2)
        
        good, = np.where(all_filtered.mask == False)
        
        full_time_diffs = np.ma.zeros(len(all_filtered), dtype = int)
        full_time_diffs.mask = copy.deepcopy(all_filtered.mask[:])
        full_time_diffs[good[:-1]] = station.time.data[good[1:]] - station.time.data[good[:-1]]
        
        # develop critical values using clean values
        # NOTE 4/7/14 - make sure that Missing and Flagged values treated appropriately
        print "sort the differencing if values were flagged rather than missing"

        full_filtered_diffs = np.ma.zeros(len(all_filtered))
        full_filtered_diffs.mask = copy.deepcopy(all_filtered.mask[:])
        full_filtered_diffs[good[:-1]] = all_filtered.compressed()[1:] - all_filtered.compressed()[:-1]
        
        # test all values
        good_to_uncompress, = np.where(st_var.data.mask == False)
        full_value_diffs = np.ma.zeros(len(st_var.data))
        full_value_diffs.mask = copy.deepcopy(st_var.data.mask[:])
        full_value_diffs[good_to_uncompress[:-1]] = st_var.data.compressed()[1:] - st_var.data.compressed()[:-1]

        # convert to compressed time to match IDL
        value_diffs = full_value_diffs.compressed()
        time_diffs = full_time_diffs.compressed()
        filtered_diffs = full_filtered_diffs.compressed()
        flags = flags[good_to_uncompress]
        

        critical_values = np.zeros([9,12])
        critical_values.fill(st_var.mdi)
        
        # link observation to calendar month
        month_locs = np.zeros(full_time_diffs.shape, dtype = int)
                
        for month in range(12):
            for year in range(month_ranges.shape[0]):
                
                if year == 0:
                    this_month_time_diff = full_time_diffs[month_ranges[year,month,0]:month_ranges[year,month,1]]
                    this_month_filtered_diff = full_filtered_diffs[month_ranges[year,month,0]:month_ranges[year,month,1]]
                else:
                    this_month_time_diff = np.ma.concatenate([this_month_time_diff, full_time_diffs[month_ranges[year,month,0]:month_ranges[year,month,1]]])
                    this_month_filtered_diff = np.ma.concatenate([this_month_filtered_diff, full_filtered_diffs[month_ranges[year,month,0]:month_ranges[year,month,1]]])


                month_locs[month_ranges[year,month,0]:month_ranges[year,month,1]] = month
      
            for delta in range(1,9):
                
                locs = np.ma.where(this_month_time_diff == delta)
        
                if len(locs[0]) >= 100:
                    
                    iqr = utils.IQR(this_month_filtered_diff[locs])

                    if iqr == 0. and delta == 1:
                        critical_values[delta-1,month] = 6.
                    elif iqr == 0: 
                        critical_values[delta-1,month] = st_var.mdi
                    else:
                        critical_values[delta-1,month] = 6. * iqr      

                    # January 2015 - changed to dynamically calculating the thresholds if less than IQR method ^RJHD

                    if plots:
                        import calendar
                        title = "{}, {}-hr differences".format(calendar.month_name[month+1], delta)                  
                        line_label = st_var.name
                        xlabel = "First Difference Magnitudes"
                    else:
                        title, line_label, xlabel = "","",""

                    threshold = utils.get_critical_values(this_month_filtered_diff[locs], binmin = 0, binwidth = 0.5, plots = plots, diagnostics = diagnostics, title = title, line_label = line_label, xlabel = xlabel, old_threshold = critical_values[delta-1,month])

                    if threshold < critical_values[delta-1,month]: critical_values[delta-1,month] = threshold

                    if plots or diagnostics:

                        print critical_values[delta-1,month] , iqr, 6 * iqr
           

        month_locs = month_locs[good_to_uncompress]
        if diagnostics:
            print critical_values[0,:]
                
        # not less than 5x reporting accuracy
        good_critical_values = np.where(critical_values != st_var.mdi)
        low_critical_values = np.where(critical_values[good_critical_values] <= 5.*reporting_resolution)
        temporary = critical_values[good_critical_values]
        temporary[low_critical_values] = 5.*reporting_resolution
        critical_values[good_critical_values] = temporary
        
        
        if diagnostics:
            print critical_values[0,:], 5.*reporting_resolution

        # check hourly against 2 hourly, if <2/3 the increase to avoid crazy rejection rate
        for month in range(12):
            if critical_values[0,month] != st_var.mdi and critical_values[1,month] != st_var.mdi:
                if critical_values[0,month]/critical_values[1,month] <= 0.66:
                    critical_values[0,month] = 0.66 * critical_values[1,month]
        
        if diagnostics:
            print "critical values"
            print critical_values[0,:]


        # get time differences for unfiltered data

        full_time_diffs = np.ma.zeros(len(st_var.data), dtype = int)
        full_time_diffs.mask = copy.deepcopy(st_var.data.mask[:])
        full_time_diffs[good_to_uncompress[:-1]] = station.time.data[good_to_uncompress[1:]] - station.time.data[good_to_uncompress[:-1]]
        time_diffs = full_time_diffs.compressed()

        # go through each difference, identify which month it is in if passes spike thresholds 
    
        # spikes at the beginning or ends of sections
        for t in np.arange(len(time_diffs)):
            if (np.abs(time_diffs[t - 1]) > 240) and (np.abs(time_diffs[t]) < 3):
                # 10 days before but short gap thereafter
                
                next_values = st_var.data[good_to_uncompress[t + 1:]]
                good, = np.where(next_values.mask == False)
        
                next_median = np.ma.median(next_values[good[:10]])
        
                next_diff = np.abs(value_diffs[t]) # out of spike
                median_diff = np.abs(next_median - st_var.data[good_to_uncompress[t]]) # are the remaining onees
                       
                if (critical_values[time_diffs[t] - 1, month_locs[t]] != st_var.mdi):
                    
                    # jump from spike > critical but average after < critical / 2
                    if (np.abs(median_diff) < critical_values[time_diffs[t] - 1, month_locs[t]] / 2.) and\
                        (np.abs(next_diff) > critical_values[time_diffs[t] - 1, month_locs[t]]) :
                    
                        flags[t] = 1
                        if plots or diagnostics:
                            sc_diagnostics_and_plots(station.time.data, st_var.data, good_to_uncompress[t], good_to_uncompress[t+1], start, variable, plots = plots)
                        
                        
            elif (np.abs(time_diffs[t - 1]) < 3) and (np.abs(time_diffs[t]) > 240):
                # 10 days after but short gap before
                
                prev_values = st_var.data[good_to_uncompress[:t - 1]]
                good, = np.where(prev_values.mask == False)
        
                prev_median = np.ma.median(prev_values[good[-10:]])
        
                prev_diff = np.abs(value_diffs[t - 1])
                median_diff = np.abs(prev_median - st_var.data[good_to_uncompress[t]])
        
                if (critical_values[time_diffs[t - 1] - 1, month_locs[t]] != st_var.mdi):
                    
                    # jump into spike > critical but average before < critical / 2
                    if (np.abs(median_diff) < critical_values[time_diffs[t - 1] - 1, month_locs[t]] / 2.) and\
                        (np.abs(prev_diff) > critical_values[time_diffs[t - 1] - 1, month_locs[t]]) :
                    
                        flags[t] = 1
                        if plots or diagnostics:
                            sc_diagnostics_and_plots(station.time.data, st_var.data, good_to_uncompress[t], good_to_uncompress[t+1], start, variable, plots = plots)
                        
        
        
        
        ''' this isn't the nicest way, but a direct copy from IDL
            masked arrays might help remove some of the lines
            Also, this is relatively slow'''
            
        for t in np.arange(len(time_diffs)):
            for spk_len in [1,2,3]:
                if t >= spk_len and t < len(time_diffs) - spk_len:
                    
                    # check if time differences are appropriate, for multi-point spikes
                    if (np.abs(time_diffs[t - spk_len]) <= spk_len * 3) and\
                    (np.abs(time_diffs[t]) <= spk_len * 3) and\
                    (time_diffs[t - spk_len - 1] - 1 < spk_len * 3) and\
                    (time_diffs[t + 1] - 1 < spk_len * 3) and \
                    ((spk_len == 1) or \
                    ((spk_len == 2) and (np.abs(time_diffs[t - spk_len + 1]) <= spk_len * 3)) or \
                    ((spk_len == 3) and (np.abs(time_diffs[t - spk_len + 1]) <= spk_len * 3) and (np.abs(time_diffs[t - spk_len + 2]) <= spk_len * 3))):
                        
                        # check if differences are valid                        
                        if (value_diffs[t - spk_len] != st_var.mdi) and \
                        (value_diffs[t - spk_len] != st_var.fdi) and \
                        (critical_values[time_diffs[t - spk_len] - 1, month_locs[t]] != st_var.mdi):
                        
                            # if exceed critical values
                            if (np.abs(value_diffs[t - spk_len]) >= critical_values[time_diffs[t - spk_len] - 1, month_locs[t]]):

                                # are signs of two differences different
                                if (math.copysign(1, value_diffs[t]) != math.copysign(1, value_diffs[t - spk_len])):
                                    
                                    # are within spike differences small
                                    if (spk_len == 1) or\
                                    ((spk_len == 2) and (np.abs(value_diffs[t - spk_len + 1]) < critical_values[time_diffs[t - spk_len + 1] -1, month_locs[t]] / 2.)) or \
                                    ((spk_len == 3) and (np.abs(value_diffs[t - spk_len + 1]) < critical_values[time_diffs[t - spk_len + 1] -1, month_locs[t]] / 2.) and\
                                      (np.abs(value_diffs[t - spk_len + 2]) < critical_values[time_diffs[t - spk_len + 2] -1, month_locs[t]] / 2.)):
                                    
                                        # check if following value is valid
                                        if (value_diffs[t] != st_var.mdi) and (critical_values[time_diffs[t] - 1, month_locs[t]] != st_var.mdi) and\
                                            (value_diffs[t] != st_var.fdi):
                                            
                                            # and if at least critical value                                            
                                            if (np.abs(value_diffs[t]) >= critical_values[time_diffs[t] - 1, month_locs[t]]):
                                                
                                                # test if surrounding differences below 1/2 critical value
                                                if (np.abs(value_diffs[t - spk_len - 1]) <= critical_values[time_diffs[t - spk_len - 1] - 1, month_locs[t]] / 2.): 
                                                    if (np.abs(value_diffs[t + 1]) <= critical_values[time_diffs[t + 1] - 1, month_locs[t]] / 2.): 
                                                    
                                                        # set the flags
                                                        flags[ t - spk_len + 1 : t +1] = 1   

                                                        if plots or diagnostics:
                                                            
                                                            sc_diagnostics_and_plots(station.time.data, st_var.data, good_to_uncompress[t-spk_len+1], good_to_uncompress[t+1], start, variable, plots = plots)
                                                           

        station.qc_flags[good_to_uncompress, flag_col[v]] = flags
                                    
        flag_locs, = np.where(station.qc_flags[:, flag_col[v]] != 0)

        utils.print_flagged_obs_number(logfile, "Spike", variable, len(flag_locs), noWrite = diagnostics) # additional flags


        # copy flags into attribute
        st_var.flags[flag_locs] = 1
 
        # matches 030660 - but with adapted IDL
        # matches 030220 OK, but finds more but all are reasonable 1/9/14

        do_interactive = False
        if plots and do_interactive == True:
            import matplotlib.pyplot as plt
        
            plot_times = utils.times_hours_to_datetime(station.time.data, start)
            
            plt.clf()
            plt.plot(plot_times, all_filtered, 'bo', ls='-')
            flg = np.where(flags[:, flag_col[v]] == 1)
            plt.plot(plot_times[flg], all_filtered[flg], 'ro', markersize=10)
            plt.show()
	    
    station = utils.append_history(station, "Spike Check")  

    return # sc


isd_dewpt = (
    df
    .filter(pl.col("('dew point', 'temperature QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    .agg([
        pl.col(["('dew point', 'temperature')"]).mean().alias('Mean Dew Pt')
    ])
)


isd_press = (
    df
    .filter(pl.col("('sea level pressure', 'pressure QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    .agg([
        pl.col(["('sea level pressure', 'pressure')"]).mean().alias('Mean Sea Press.')
    ])
)

isd_atm = (
    df
    .filter(pl.col("('Atmospheric-Pressure-Observation 1', 'ATM_PRESS_QC')").is_in(include_qc))
    .groupby(["date", "('Control', 'USAF')","('Control', 'WBAN')"])
    .agg([
        pl.col(["('Atmospheric-Pressure-Observation 1', 'PRESS_RATE')"]).mean().alias('Mean Atm Press.')
    ])
)

###############

# dfs = [df, isd_airtemp,
#     isd_dewpt,
#     isd_press,
#     isd_atm,
#     isd_rhave,
#     isd_rh_tave,
#     isd_rhraw1, isd_rhraw2, isd_rhraw3,
#     isd_minrh_t, isd_maxrh_t, isd_sdrh_t, isd_sdrh
# ]

### 2000
isd_data = df_forjoin.join(isd_airtemp.join(isd_dewpt.join(isd_press.join(isd_atm, on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left'), 
on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left'),
on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left'),
on=["date", "('Control', 'USAF')","('Control', 'WBAN')"],
    how='left')

for col in isd_data.columns:
    print(col)

def hcc_sss(T, D, month_ranges, start, logfile, plots = False, diagnostics = False):
    '''
    Supersaturation check, on individual obs, and then if >20% of month affected
    
    :param array T: temperatures
    :param array D: dewpoint temperatures
    :param array month_ranges: array of month start and end times
    :param datetime start: DATASTART (for plotting)
    :param file logfile: logfile to store outputs
    :param bool plots: do plots or not
    :param bool diagnostics: extra verbose output
    :returns: flags - locations where flags have been set
    '''
    
    flags = np.zeros(len(T))

    # flag each location where D > T
    for m,month in enumerate(month_ranges):

        data_count = 0.
        sss_count = 0.

        try:
            for t in np.arange(month[0],month[1]):

                data_count += 1

                if D[t] > T[t]:
                    sss_count += 1

                    flags[t] = 1

                    if plots:
                        hcc_time_plot(T, D, t-1, t, start)
        except IndexError:
            # no data for that month - incomplete year
            pass

        # test whole month
        # if more than 20% flagged, flag whole month
        if sss_count / data_count >= SSS_MONTH_FRACTION:
            
            flags[month[0]:month[1]] = 1

            if plots:
                hcc_time_plot(T, D, month[0], month[1], start)

    nflags = len(np.where(flags != 0)[0])
    utils.print_flagged_obs_number(logfile, "Supersaturation", "temperature", nflags, noWrite = diagnostics)

    # not yet tested.
    return flags # hcc_sss

### Add temp diff column
isd_data = isd_data.with_columns([
    (pl.col("Mean Air Temp.") - pl.col("Mean Dew Pt")).alias('Mean Temp Diff')
])

isd = isd_data.to_pandas()

### Add RH column

RH = []
metpy_RH = []

for i in range(0,isd.shape[0]):
    num = math.exp(17.625 * isd["Mean Dew Pt"].loc[i]/(243.04 + isd["Mean Dew Pt"].loc[i]))
    den = math.exp(17.625 * isd["Mean Air Temp."].loc[i]/(243.04 + isd["Mean Air Temp."].loc[i]))
    metpy_rh = 100*metpy.calc.relative_humidity_from_dewpoint(isd["Mean Air Temp."].loc[i] * units.degC, isd["Mean Dew Pt"].loc[i] * units.degC)
    out = 100 * (num/den)
    
    RH.append(out)
    metpy_RH.append(float(metpy_rh))
    
isd["Calculated RH"] = RH    
isd["MetPy RH"] = metpy_RH

isd.to_parquet('s3://ncai-humidity/isd/daily/'+str(mystate)+"-"+str(myyr)+'-DAILY.parquet',index=False)

isd.to_parquet('s3://ncai-humidity/isd/daily/'+str(mystate)+"-ALLYEARS-DAILY.parquet",index=False)

### END


