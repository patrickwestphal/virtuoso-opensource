#!/bin/bash

SCALE=${1-1}
STREAMS=${2-1}
LOOPS=${3-2}
LOGFILE=`pwd`/dbgen.output
export LOGFILE
. $HOME/binsrc/tests/tpc-h/test_fn.sh
DBGEN_PROG=dbgen
cd dbgen
BANNER "PREPARE TPC-H DATA"
make
if [ ! -x $DBGEN_PROG ]
then
    LOG "No "$DBGEN_PROG" executable compiled. Exiting"
    CHECK_LOG
    BANNER "COMPLETED (load.sh)"
    exit 0
else
    LOG "The "$DBGEN_PROG" is OK."
fi
./$DBGEN_PROG -fq -s $SCALE #>dbgen.out 2>dbgen.out
./$DBGEN_PROG -qf -s $SCALE -U `expr \( $STREAMS + 1 \) \* $LOOPS` #>dbgen.out 2> dbgen.out
chmod +r *.tbl*
cd ..
BANNER "COMPLETED TPC-H DATA"
