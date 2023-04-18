#!/bin/csh

set YYYY = 2006

foreach month (01 02 03 04 05 06 07 08 09 10 11 12)

foreach day (01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31)

set filename = HIRS_AtmosphericProfiles_V5_M02_{$YYYY}{$month}{$day)_int.nc

begin

  fname = "$filename"

  f = addfile(fname, "r")

  x = f->VARNAMEHERE(:,0)

end

echo "$filename done"

end

end
