import numpy as np
import xarray as xr

from ...import HRRRZARR

CHUNK_URL = f"s3://{HRRRZARR}/grid/HRRR_chunk_index.zarr"

def windspeed( dataset, components=['UGRD', 'VGRD'], **kwargs ):
    """
    Compute wind speed from u- and v-components
    
    Arguments:
      dateset (xarray.Dataset) : Data to compute wind
        speed from

    Keyword arguments:
      components (list, tuple) : Names of the wind component variables
      All others silently ignored
            
    Returns:
      xarray.Dataset : Updated Dataset that contains wind
        speed
    
    """
    
    att   = 'long_name'

    spd   = [dataset[var]*dataset[var] for var in components]
    spd   = np.sqrt( spd[0] + spd[1] )
    attrs = dataset[ components[0] ].attrs
    if att in attrs:
        attrs[att] = '/'.join( attrs[att].split('/')[:-1] ) + '/SPD'
    return dataset.drop( components ).assign( SPD = spd.assign_attrs( attrs ) )

def stats( dataset, dim='time', keep_attrs=True, **kwargs ):
    """
    Compute various statitics on group Datasets
    
    Given an xarray.Dataset, compute the min, max, and mean of
    all DataArrays in the Dataset. At the same  time, rename the
    variables in the Dataset so that they reflect the statistic
    they represent (e.g.,  '_min' is appended to each DataArray
    name for minimum values). All statistics are then merged back
    into a single Dataset and returned
    
    Arguments:
      dataset (xarray.Dataset) : Data to compute statistics for
    
    Keyword arguments:
      dim (str) : Dimension along which to compute statistics.
      keep_attrs (bool) : If set, attributes will be pulled
        forward into new values.
      **kwargs : These keywords are passed to the .min(), .max(), and
        .mean() methods when when compute statistics
    
    Return:
       xarray.Dataset : New Dataset containing statistics stored under
         renamed  variables
    
    """
    
    keys = list( dataset.keys() )
    return xr.merge( [
        dataset.min(  dim=dim, keep_attrs=keep_attrs ).rename( {k:f"{k}_min"  for k in keys} ),
        dataset.max(  dim=dim, keep_attrs=keep_attrs ).rename( {k:f"{k}_max"  for k in keys} ),
        dataset.mean( dim=dim, keep_attrs=keep_attrs ).rename( {k:f"{k}_mean" for k in keys} ),
    ] )

def getChunkData( ):

    return xr.open_zarr( CHUNK_URL )
