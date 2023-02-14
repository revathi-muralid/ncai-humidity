	SUBROUTINE DENSUB ( )

CC Declare MD data array and some specific key position array 
        real*8 ra(5),rb(2),rc(2,2),rd(2,2),re(2),rf(2),rg(3),
     *            rh(4),ri(2,72),rj(8),rk(7),rl(3),rm(8,41)


CCC Max number of mnemonics in CMNSTR
        parameter (MXMN = 400 )

CC Max levels of a mnemonic OR Max repeat times 
        parameter (MXLV = 86 )

        real*8  r8arr( MXMN, MXLV), r8(5)
 
        Parameter ( MXBF=16000)
  	character cbfmsg*(MXBF), csubset*8, cmnstr*200

	integer*4   ibfmsg( MXBF / 4 )

CC Let cbfmsg and ibfmsg share the same Memory address
      	equivalence ( cbfmsg(1:4), ibfmsg(1) ) 

CC Data in MD file : Integer value (scaled ) , Real value ( unscaled )
        integer imdata(400)
        real*8  rmdata(400)

CC use COMMON to delieve data from main code to here
        common /mdrecrod / imdata, rmdata 

CC
CC----------------  subroutine starts here  ------------------
CC

CCC  Store values into NESDIS internal BUFR message array 
CCC  value-level: 4th step 

CC* YEAR DOYR HOUR MINU SECO
CC ra(5) 

        call ufbrep(11, ra, 5, 1, nlv, 'YEAR DOYR HOUR MINU SECO')
        print *, 'ra', ra

CC* SAID SIDU
CC  rb(2)

        call ufbint(11, rb, 2, 1, nlv, 'SAID SIDU' )
        print *,'rb',rb


CC* DOYR HOUR DOYR HOUR
CC rc(2,2)  DDDHH

        call ufbrep(11, rc, 2,2, nlv, 'DOYR HOUR')
        print  *  , 'rc', rc

CC* DLAT DLON DLAT DLON
CC rd(2,2)     XXYYY

        call ufbrep(11, rd, 2,2, nlv, 'DLAT DLON')
        print * , 'rd', rd

CC* YEAR DOYR
CC re(2)

        call ufbint(11, re, 2,1, nlv, 'YEAR DOYR')
        print *, 're', re

CC* PLAT PLON
CC rf(2)

        call ufbint(11, rf, 2,1 , nlv, 'CLATH CLONH')
        print *, 'rf', rf

CC* HOUR MINU SECO
CC  rg(3)

        call ufbint(11, rg, 3,1, nlv, 'HOUR MINU SECO')
        print *, 'rg', rg

CC* QMRK GSCU TCSF DINU
CC rh(4)

        call ufbint(11, rh, 4, 1, nlv, 'QMRK GSCU TCSF DINU')
        print *, 'rh', rh

CC* GBTC TMBR
CC ri(72,2)

        call ufbrep(11, ri, 2, 72, nlv, 'GBTC TMBR')
        print *, 'ri ' , ri

CC* BDRF SOEL ELEV CLAM CDTP VSAT GCDTT GSDP
CC rj(8)

        call ufbint(11, rj, 8, 1, nlv, 'BDRF SOEL ELEV CLAM
     *       CDTP VSAT GCDTT GSDP')
        print  *, 'rj  ' , rj

CC* TMSK TPWT GLFTI GSPC TPWT GSPC GLFTI
CC rk(7)

        call ufbint(11, rk, 7,1, nlv, 'TMSK TPWT GLFTI GSPC
     *        TPWT GSPC GLFTI')
        print *,'rk ' ,  rk

CC* TPWT TPWT TPWT
CC rl(3)

        call ufbrep(11, rl, 1, 3, nlv, 'TPWT')
	print *, 'rl', rl 

CC* PRLC TMDB TMDP HITE GSPC TMDB GSPC TMDP
CC rm(8, 41)

        call ufbrep(11, rm,8, 41, nlv, 'PRLC TMDB TMDP 
     *      HITE GSPC TMDB GSPC TMDP')
	do j=1,41
          print *, ' rm ',j,'  ' , ( rm(i,j), i =1, 8)
	end do


	RETURN 
	END 	
