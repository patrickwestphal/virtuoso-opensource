#!/bin/sh
#  
#  $Id$
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

# Note: datasets for TPC-H and its RDF variant (RDFH) are generated 
# and then cached in directories 
#   $VIRTUOSO_TEST/tpch/dataset.sql 
# and 
#   $VIRTUOSO_TEST/tpch/dataset.rdf
# These datasets are not regenerated each time the tests are run.
# If you have some problems with these data or need to change TPC-H scale,
# you need to erase one# or both of those subdirectories manually 
# for the datasets to be regenerated.

DSN=$PORT
. $VIRTUOSO_TEST/testlib.sh

testdir=`pwd`
LOGFILE=$testdir/ttpch.output
tpch_scale=1
#0.25
#rdfhgraph="http://example.com/tpcd"
rdfhgraph="urn:example.com:tpcd"
nrdfloaders=6
nsqlloaders=6
dbgendir=$VIRTUOSO_TEST/../tpc-h/dbgen206
dbgen=$dbgendir/dbgen
bibm=$VIRTUOSO_TEST/../bibm

LOCAL=$PORT
#GENERATE_PORTS 1
#REMOTE=$GENERATED_PORT
if [ $REMOTE -eq $LOCAL ]
then
    REMOTE=`expr $LOCAL + 1`
fi


# SQL command 
DoCommand()
{
  _dsn=$1
  command=$2
  shift 
  shift
  echo "+ " $ISQL $_dsn dba dba ERRORS=STDOUT VERBOSE=OFF PROMPT=OFF "EXEC=$command" $*	>> $LOGFILE	
  $ISQL $_dsn dba dba ERRORS=STDOUT VERBOSE=OFF PROMPT=OFF "EXEC=$command" $* >> $LOGFILE
  if test $? -ne 0 
  then
    LOG "***FAILED: $command"
  else
    LOG "PASSED: $command"
  fi
}

BANNER "STARTED TPC-H/RDF-H functional tests"
NOLITE
if [ "z$NO_TPCH" != "z" ]
then
   exit
fi 

STOP_SERVER $LOCAL
mem=`sysctl hw.memsize | cut -f 2 -d ' '`
if [ -f /proc/meminfo ]
then
mv virtuoso-1111.ini virtuoso-tmp.ini
NumberOfBuffers=`cat /proc/meminfo | grep "MemTotal" | awk '{ MLIM = 524288; m = int($2 / 4 / 8 / 2); if(m > MLIM) { m = MLIM; } print m; }'`
sed "s/NumberOfBuffers\s*=\s*2000/NumberOfBuffers         = $NumberOfBuffers/g" virtuoso-tmp.ini > virtuoso-1111.ini 
elif [ "z$mem" != "z" ]
then
mv virtuoso-1111.ini virtuoso-tmp.ini
NumberOfBuffers=`sysctl hw.memsize | awk '{ MLIM = 524288; m = int($2 / 4 / 8 / 1024 / 4); if(m > MLIM) { m = MLIM; } print m; }'`
sed -e "s/NumberOfBuffers.*=.*2000/NumberOfBuffers         = $NumberOfBuffers/g" virtuoso-tmp.ini > virtuoso-1111.ini 
fi
MAKECFG_FILE $TESTCFGFILE $PORT $CFGFILE
cd $testdir

if [ ! -d $VIRTUOSO_TEST/tpch ]
then
    mkdir $VIRTUOSO_TEST/tpch
fi

# generate TPC-H dataset
if [ ! -d $VIRTUOSO_TEST/tpch/dataset.sql ]
then
    mkdir $VIRTUOSO_TEST/tpch/dataset.sql 
fi
if [ -d $VIRTUOSO_TEST/tpch/dataset.sql ]
then
    LOG "TPC-H data generation scale=$scale started at `date`" 
    cd $VIRTUOSO_TEST/tpch/dataset.sql
    ln -s $dbgendir/dists.dss .

    RUN $dbgen -fFv -s $tpch_scale
    if [ $STATUS -ne 0 ]
    then
	LOG "***ABORTED: dbgen -- TPC-H data generation."
	exit 1
    fi

    RUN chmod u=rw,g=rw,o=r $VIRTUOSO_TEST/tpch/dataset.sql/*
    if [ $STATUS -ne 0 ]
    then
        LOG "***ABORTED: can't give permissions on DBGEN-generated files."
        exit 1
    fi
    
    LOG "TPC-H data generation finished at `date`"  
    cd $testdir
    ln -s $VIRTUOSO_TEST/tpch/dataset.sql .
    ln -s $VIRTUOSO_TEST/tpch/dataset.sql src
fi

START_SERVER $PORT 1000
cd $testdir

# load TPC-H tables
LOG "Loading SQL data for TPC-H ..."
RUN $ISQL $DSN dba dba ERRORS=STDOUT VERBOSE=OFF PROMPT=OFF $bibm/tpch/virtuoso/schema.sql 
if [ $STATUS -ne 0 ]
then
    LOG "***ABORTED: schema.sql -- Loading SQL schema for TPCH."
    exit 1
fi
RUN $ISQL $DSN dba dba ERRORS=STDOUT VERBOSE=OFF PROMPT=OFF $bibm/tpch/virtuoso/ldschema.sql 
if [ $STATUS -ne 0 ]
then
    LOG "***ABORTED: ldschema.sql -- Loading SQL schema for TPCH."
    exit 1
fi

LOG "TPC-H data loading started at `date`" 
RUN $ISQL $DSN dba dba ERRORS=STDOUT VERBOSE=OFF PROMPT=OFF $bibm/tpch/virtuoso/ldfile.sql
if [ $STATUS -ne 0 ]
then
    LOG "***ABORTED: ldfile.sql -- Loading SQL schema for TPCH."
    exit 1
fi

$ISQL $DSN dba dba ERRORS=STDOUT VERBOSE=OFF PROMPT=OFF < $bibm/tpch/virtuoso/ld.sql >> $LOGFILE
if [ $STATUS -ne 0 ]
then
    LOG "***ABORTED: ld.sql -- Loading SQL schema for TPCH."
    exit 1
fi

LOG "TPC-H data loading finished at `date`" 

CHECKPOINT_SERVER
LOG "Checking data after loading" 
RUN $ISQL $DSN dba dba ERRORS=STDOUT VERBOSE=OFF PROMPT=OFF $VIRTUOSO_TEST/ttpch-load-check.sql -u TPCH_SCALE=$tpch_scale
if [ $STATUS -ne 0 ]
then
    LOG "***ABORTED: ttpch-load-check.sql -- Checking data for TPCH."
    exit 1
fi

LOG "SQL data for TPC-H loaded."

# run TPC-H test (qualification)
LOG "running TPC-H qualification over JDBC ..."
RUN $bibm/tpchdriver -sql -err-log err.log -dbdriver virtuoso.jdbc4.Driver jdbc:virtuoso://localhost:$PORT/UID=dba/PWD=dba -t 300000 -scale $tpch_scale -uc tpch/sql -defaultparams -q -printres -mt 1 
if [ $STATUS -ne 0 ]
then
    LOG "***ABORTED: TPC-h qual"
    exit 1
fi
LOG "TPC-H qualification complete."
mv run.log tpch.log
mv run.qual tpch.qual

SHUTDOWN_SERVER $LOCAL

LOG ""
LOG "Validating TPCH results agains qualification data."
RUN $bibm/compareresults.sh $bibm/tpch/valid.qual tpch.qual 

CHECK_LOG
BANNER "COMPLETED SERIES OF TPC-H/RDF-H TESTS"

