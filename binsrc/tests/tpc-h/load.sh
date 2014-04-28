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

LOGFILE=`pwd`/load.output
export LOGFILE

SCALE=${1-1}
#THREADS=${2-1}
#LOOPS=${3-2}
WO_START=${WO_START-1}

if [ "$#" -gt 0 ]
then
    echo
    echo "TPC-H LOAD " 
    echo "SCALE:" $SCALE
#    echo "STREAMS:" $THREADS
    echo
else
    echo
    echo "     load.sh scale"
    echo
    echo "     export WO_START=1 in order to run against existing instance " 
    echo 
    echo "     load.sh 1"
    exit
fi

echo Using virtuoso configuration | tee -a $LOGFILE
SERVER=virtuoso-t
CFGFILE=virtuoso.ini
DBFILE=virtuoso.db
DBLOGFILE=virtuoso.trx
DELETEMASK="virtuoso.lck $DBLOGFILE $DBFILE virtuoso.tdb virtuoso.ttr"
SRVMSGLOGFILE="virtuoso.log"
TESTCFGFILE=virtuoso-1111.ini
BACKUP_DUMP_OPTION=+backup-dump
CRASH_DUMP_OPTION=+crash-dump
FOREGROUND_OPTION=+foreground
LOCKFILE=virtuoso.lck
export FOREGROUND_OPTION LOCKFILE
export CFGFILE DBFILE DBLOGFILE DELETEMASK SRVMSGLOGFILE 
export BACKUP_DUMP_OPTION CRASH_DUMP_OPTION TESTCFGFILE SERVER

. $HOME/binsrc/tests/tpc-h/test_fn.sh

DBGEN_PROG=dbgen

BANNER "STARTED load.sh"


if [ $WO_START -eq 0 ]
then
    STOP_SERVER
    rm -f $DBLOGFILE
    rm -f $DBFILE
    MAKECFG_FILE $TESTCFGFILE $PORT $CFGFILE
    START_SERVER $PORT 1000
fi

LOG "=  LOADING TPC-H DATA " "`date \"+%m/%d/%Y %H:%M:%S\"`"

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < schema.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: schema.sql"
    exit 3
fi

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < ldschema.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: ldschema.sql"
    exit 3
fi

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < tpc-h.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: tpc-h.sql"
    exit 3
fi

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < ldfile.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: ldfile.sql"
    exit 3
fi
RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < ld.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: ld.sql"
    exit 3
fi
BANNER "FINISHED LOADING TPC-H DATA " "`date \"+%m/%d/%Y %H:%M:%S\"`"

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT -u SCALE=$SCALE < ldck.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: ldck.sql"
    exit 3
fi

CHECK_LOG
BANNER "COMPLETED Load TPC-H data."
