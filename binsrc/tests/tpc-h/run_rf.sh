#!/bin/sh
#
#  $Id$
#
#  choose a server to run with
#  
#  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
#  project.
#  
#  Copyright (C) 1998-2013 OpenLink Software
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

# 0	    1 	  2 	  3 	   4      5 	   6 	     
# run_rf.sh $ISQL $DSN[1] $stream  $LOOP  $THREADS $LOGFILE

ISQL=$1
DSN=$2
STREAM=$3
SEQ=$4
THREADS=$5
LOGFILE=$6
POW=$7
CLI=$8
EXT=rf-$3.$4

LOG()
{
    echo "$*"	| tee -a $LOGFILE | tee -a $LOGFILE.$EXT
}

LOG "PASSED: START RF STREAM "$3" AT "$2 "`date \"+%m/%d/%Y %H:%M:%S\"`"
s_count=0 
while [ "$s_count" -lt $THREADS ]
do
    s_count=`expr $s_count + 1`
    if [ $POW -gt 0 ]
    then
	stream=0
    else
	stream=`expr $s_count % $THREADS + 1`
    fi
    q=22
    while [ "$q" -lt "24" ]
    do
	q=`expr $q + 1`
	etext="rf_metric ($SEQ, $q, $stream, $CLI)"
	#echo $etext
	$ISQL $DSN dba dba EXEC="$etext" >> $LOGFILE.$EXT 
	if test $STATUS -ne 0
	then
	    LOG "***FAILED: " $seq, $q, $STREAM " AT "$DSN
	fi
    done    
done    

LOG "PASSED: FINISH RF STREAM "$STREAM" AT "$DSN "`date \"+%m/%d/%Y %H:%M:%S\"`"
