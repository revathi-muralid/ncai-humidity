# Created on: 5/7/2023 by RM
# Last updated: 5/7/2023 by RM

from colocator import ISDDat, HirsDat
import pandas as pd
import numpy as np
import awswrangler as wr

for k in range(231,328):
	d = ISDDat()
	e = d.get_isd(k)
	f=HirsDat(d)
	g=f.get_hirs()
	h=f.isd_time(g,e)
	h.astype(str).to_parquet("s3://ncai-humidity/matching/HIRS_time/"+d.isdname+".parquet")
