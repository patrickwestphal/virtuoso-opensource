#!/bin/sh
#
#  $Id$
#
#  choose a server to run with
#  
#  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
#  project.
#  
#  Copyright (C) 1998-2014 OpenLink Software
#  
#  This project is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; only version 2 of the License, dated June 1991.
#  
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#  General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#  
#  

#. $HOME/binsrc/tests/suite/test_fn.sh

ISQL=$1
DSN=$2
STREAM=$3
SEQ=$4
LOGFILE=$6
POW=${7-0}
EXT=$3

LOG()
{
    echo "$*"	| tee -a $LOGFILE | tee -a $LOGFILE.$EXT
}

# 0		    1 	  2 	  3 	   4      5 	   6 
# run_one_client.sh $ISQL $DSN 	  $STREAM $LOOP  $THREADS $LOGFILE


if [ $POW -lt 1 ]
then
    STREAM=`expr $STREAM + 1`
fi
LOG "PASSED: START STREAM "$STREAM" RUN #$SEQ AT "$DSN "`date \"+%m/%d/%Y %H:%M:%S\"`"
q=0
while [ "$q" -lt "22" ]
do
    q=`expr $q + 1`
    #etext="metric ($SEQ, $q, $STREAM)"
    #echo $etext
    #$ISQL $DSN dba dba EXEC="$etext" >> $LOGFILE.$EXT
    $ISQL $DSN dba dba qset_"$STREAM"_"$q".sql >> $LOGFILE.$EXT
    if test $STATUS -ne 0
    then
	LOG "***FAILED: " $SEQ, $q " AT " $DSN
    fi
done    

LOG "PASSED: FINISH STREAM "$STREAM" RUN #$SEQ AT "$DSN "`date \"+%m/%d/%Y %H:%M:%S\"`"
