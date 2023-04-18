#!/bin/bash

files = 'HIRS_AtmosphericProfiles_V5_M02_2017262_int.nc'

for file in $files ; do
	cdo sellonlatbox, -94.617919,-75.242266,24.523096,39.466012 $file /home/rmurali/HIRS/HIRS_SE_USA/test.nc
done
