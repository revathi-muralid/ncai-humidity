############################################################
# Name:     denbufr.sh
# Purpose:  Recompile and run the decoder of NCEP Bufr file
# Input:    bufrfile_NCEP
# Output:   Standard output
# Exmaple:  denbufr.sh > abc
#
############################################################

#rm ../bin/denmain.k 

subs="densub.o"

#cd ../src

mccomp  -O3 -C -g77 denmain.pgm
mccomp  -O3 -C -g77 densub.for
mccomp  -O3 -C -g77 -o  ../bin/denmain.k denmain.o densub.o /home/mcidas/lib/main.o  -L /home/mcidas/lib -lmcidas -L ../lib  -lbufroper -L. -lsfovoper

rm denmain.o densub.o 

#cd ../data
#denmain.k  
