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

class satDat():
    def __init__(self, fname):
        self.fname = fname
    def read(self) :
        self.hdf = SD('/tmp/%s'%self.fname, SDC.READ)
        
        # Print dataset names
        
        self.ds_dic = self.hdf.datasets()
        self.my_dic = dict((k, self.ds_dic[k]) for k in ('Latitude','Longitude','Scan_Start_Time','Cloud_Mask','Surface_Pressure','Surface_Elevation','Processing_Flag','Retrieved_Temperature_Profile','Retrieved_Moisture_Profile','Water_Vapor','Water_Vapor_Low','Water_Vapor_High'))
        
        # Deal with 'Quality_Assurance' variable separately
        
        self.vars = {}
        for idx,sds in enumerate(self.my_dic.keys()):
            self.x = self.hdf.select(sds)
            self.xs = self.x.get()
            self.xs = pd.DataFrame(self.xs.flatten())
            self.vars[sds] = self.xs
            print (idx,sds)
        
        self.qa = self.hdf.select('Quality_Assurance')
        self.qas = self.qa.get()
        self.qas_flat = self.qas.flatten()
        
        self.qa_fordf=["".join([str(self.qas_flat[i]), str(self.qas_flat[i+1]),str(self.qas_flat[i+2]), str(self.qas_flat[i+3]),str(self.qas_flat[i+4]),str(self.qas_flat[i+5]),str(self.qas_flat[i+6]), str(self.qas_flat[i+7]),str(self.qas_flat[i+8]), str(self.qas_flat[i+9])]) for i in range((self.qas_flat.shape[0])-9)]
        
        self.qa_fordf=pd.DataFrame(self.qa_fordf[::10])
        
        self.vars['Quality_Assurance']=self.qa_fordf

        self.dfind = [self.qa_fordf[0][q][0:2]=='51' for q in range(self.qa_fordf.shape[0])] 

        self.qa_fordf = self.qa_fordf[self.dfind] 
        
        # Set NaNs
        self.vars['Retrieved_Temperature_Profile'][self.vars['Retrieved_Temperature_Profile']==-32768] = np.nan
        self.vars['Retrieved_Moisture_Profile'][self.vars['Retrieved_Moisture_Profile']==-32768] = np.nan
        self.vars['Surface_Elevation'][self.vars['Surface_Elevation']==-32768] = np.nan
        self.vars['Surface_Pressure'][self.vars['Surface_Pressure']==-32768] = np.nan
        self.vars['Water_Vapor'][self.vars['Water_Vapor']==-9999] = np.nan
        self.vars['Water_Vapor_Low'][self.vars['Water_Vapor_Low']==-9999] = np.nan
        self.vars['Water_Vapor_High'][self.vars['Water_Vapor_High']==-9999] = np.nan
        
        # Scale and adjust
        self.surf_press_sf = 0.1000000014901161
        self.vars['Surface_Pressure'] = self.vars['Surface_Pressure']*self.surf_press_sf

        # same sf and ao for ret temp and ret moist
        self.ret_temp_sf = 0.009999999776482582 
        self.ret_temp_ao = -15000
        self.vars['Retrieved_Temperature_Profile'] = self.ret_temp_sf*(self.vars['Retrieved_Temperature_Profile']-self.ret_temp_ao)
        self.vars['Retrieved_Moisture_Profile'] = self.ret_temp_sf*(self.vars['Retrieved_Moisture_Profile']-self.ret_temp_ao)

        self.wat_vap_sf = 0.001000000047497451
        self.vars['Water_Vapor'] = self.wat_vap_sf*self.vars['Water_Vapor']
        self.vars['Water_Vapor_Low'] = self.wat_vap_sf*self.vars['Water_Vapor_Low']
        self.vars['Water_Vapor_High'] = self.wat_vap_sf*self.vars['Water_Vapor_High']
    
        self.nrows = self.qas.shape[0]*self.qas.shape[1]
        
        ### Only keep rows for lowest pressure level

        self.vars['Retrieved_Temperature_Profile'] = self.vars['Retrieved_Temperature_Profile'][self.nrows*18:self.nrows*19]
        self.vars['Retrieved_Moisture_Profile'] = self.vars['Retrieved_Moisture_Profile'][self.nrows*18:self.nrows*19]

        ### Only keep rows that have good data quality

        self.fvars = {}
        for idx,sds in enumerate(self.my_dic.keys()):
            self.xs2 = self.vars[sds]
            self.xs2 = self.xs2[self.dfind]
            self.fvars[sds] = self.xs2
            print (idx,sds)

        self.fvars['Quality_Assurance'] = self.qa_fordf

        # Scan_Start_Time is given in seconds since 1993

        self.fvars['Scan_Start_Time']=self.fvars['Scan_Start_Time'].reset_index()

        self.orig_date = datetime.strptime('01-01-1993', '%d-%m-%Y')
        self.mytimes = [self.orig_date + timedelta(seconds=self.fvars['Scan_Start_Time'][0][x]) for x in range(len(self.fvars['Scan_Start_Time']))]

        self.fvars['Scan_Start_Time'] = pd.DataFrame(self.mytimes)
    
        self.frames = self.fvars['Latitude'].reset_index(), self.fvars['Longitude'].reset_index(), self.fvars['Scan_Start_Time'].reset_index(), self.fvars['Cloud_Mask'].reset_index(), self.fvars['Surface_Pressure'].reset_index(), self.fvars['Surface_Elevation'].reset_index(), self.fvars['Processing_Flag'].reset_index(), self.fvars['Retrieved_Temperature_Profile'].reset_index(), self.fvars['Retrieved_Moisture_Profile'].reset_index(), self.fvars['Water_Vapor'].reset_index(), self.fvars['Water_Vapor_Low'].reset_index(), self.fvars['Water_Vapor_High'].reset_index(),self.fvars['Quality_Assurance'].reset_index()

        self.df=pd.concat(self.frames, axis=1, ignore_index=True, verify_integrity=True)
        self.df=self.df[list(range(1,27,2))]

        self.mynames = list(self.my_dic.keys())
        self.mynames.append('QA')
        self.df.columns=self.mynames

        self.outname = self.fname.rsplit(".",1)[0]