# Created on: ? by RM
# Last updated: 3/23/23 by RM

import xarray
import pandas as pd
import os
import boto3
import requests
import warnings
import logging
import pyhdf.SD as SD
from pyhdf.SD import SD, SDC, SDAttr, SDS
from numpy import *
import numpy as np
from datetime import datetime
from datetime import timedelta
from itertools import product

class satDat():
    def __init__(self, fname):
        self.fname = fname
        
    def read(self,invars,myqavar) :
        self.hdf = SD('/tmp/%s'%self.fname, SDC.READ)
        self.attr = self.hdf.attributes(full=1)
        
        # Print dataset names
        
        self.ds_dic = self.hdf.datasets()
        
        self.invars=invars ### User inputs invars
        
        self.my_dic = dict((k, self.ds_dic[k]) for k in self.invars)
        
        # Deal with 'Quality_Assurance' variable separately
        
        self.vars = {}
        for idx,sds in enumerate(self.my_dic.keys()):
            self.x = self.hdf.select(sds)
            self.xs = self.x.get()
            self.xs = pd.DataFrame(self.xs.flatten())
            self.vars[sds] = self.xs
            print (idx,sds)
        
        self.myqavar = myqavar
        
        self.qa = self.hdf.select(myqavar)
        self.qas = self.qa.get()
        self.qas_flat = self.qas.flatten()
        self.nrows = self.qas.shape[0]*self.qas.shape[1]
        
        self.qa_fordf=["".join([str(self.qas_flat[i]), str(self.qas_flat[i+1]),str(self.qas_flat[i+2]), str(self.qas_flat[i+3]),str(self.qas_flat[i+4]),str(self.qas_flat[i+5]),str(self.qas_flat[i+6]), str(self.qas_flat[i+7]),str(self.qas_flat[i+8]), str(self.qas_flat[i+9])]) for i in range((self.qas_flat.shape[0])-9)]
        
        self.qa_fordf=pd.DataFrame(self.qa_fordf[::10])
        
        self.vars[myqavar]=self.qa_fordf

        self.dfind = [self.qa_fordf[0][q][0:2]=='51' for q in range(self.qa_fordf.shape[0])] 

        self.qa_fordf = self.qa_fordf[self.dfind] 
        
        # Check for datasets that have 100% bad data
        
        if self.qa_fordf.shape[0]==0:
            
            #print("This file has no good data!")
            pass
            
        else:
            
            print("You can move on to QA!")
        
    def mask_nans(self, qavar):
        #self.qavar = qavar
        mymask = self.hdf.select(qavar).getfillvalue()
        self.vars[qavar][self.vars[qavar]==mymask] = np.nan # strings are an opportunity for errors
        
    def scale_and_adjust(self, qavar, sf, ao):
        #self.sf = mysf
        #self.ao = myao
        self.vars[qavar] = sf*(self.vars[qavar] - ao)
        
    def qc_data(self, tempvar, moistvar):
        
        ### Only keep rows for lowest pressure level
        
        self.vars[tempvar] = self.vars[tempvar][self.nrows*18:self.nrows*19]
        self.vars[moistvar] = self.vars[moistvar][self.nrows*18:self.nrows*19]
        
        ### Only keep rows that have good data quality

        self.fvars = {}
        for idx,sds in enumerate(self.my_dic.keys()):
            self.xs2 = self.vars[sds]
            self.xs2 = self.xs2[self.dfind]
            self.fvars[sds] = self.xs2
            print (idx,sds)

        self.fvars[self.myqavar] = self.qa_fordf
        
    def format_time(self, timevar):
        
        # Scan_Start_Time is given in seconds since 1993

        self.fvars[timevar]=self.fvars[timevar].reset_index()

        self.orig_date = datetime.strptime('01-01-1993', '%d-%m-%Y')
        self.mytimes = [self.orig_date + timedelta(seconds=self.fvars[timevar][0][x]) for x in range(len(self.fvars[timevar]))]

        self.fvars[timevar] = pd.DataFrame(self.mytimes)
        
    def make_df(self):
        
        self.frames = [self.fvars[k].reset_index() for k in self.fvars]
        
        self.df=pd.concat(self.frames, axis=1, ignore_index=True, verify_integrity=True)
        self.ncols = self.df.shape[1]
        self.df=self.df[list(range(1,self.ncols,2))]

        self.mynames = list(self.my_dic.keys())
        self.mynames.append('QA')
        self.df.columns=self.mynames

        self.outname = self.fname.rsplit(".",1)[0]
        