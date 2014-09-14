ECHOLN BOTH $LAST[1] "END COUNT";

select curdatetime ();

select count(*) from REGION;
ECHOLN BOTH $IF $GTE $LAST[1] 5 "PASSED" "***FAILED" ": REGION table is filled properly. Should be at least 5";

select count(*) from NATION;
ECHOLN BOTH $IF $GTE $LAST[1] 25 "PASSED" "***FAILED" ": NATION table is filled properly. Should be at least 25";

select count(*) from SUPPLIER;
ECHOLN BOTH $IF $GTE $LAST[1] $* 10000 $U{SCALE} "PASSED" "***FAILED" ": SUPPLIER table is filled properly. Should be at least " $* 10000 $U{SCALE};

select count(*) from CUSTOMER;
ECHOLN BOTH $IF $GTE $LAST[1] $* 150000 $U{SCALE} "PASSED" "***FAILED" ": CUSTOMER table is filled properly. Should be at least " $* 150000 $U{SCALE};

select count(*) from PART;
ECHOLN BOTH $IF $GTE $LAST[1] $* 200000 $U{SCALE} "PASSED" "***FAILED" ": PART table is filled properly. Should be at least " $* 200000 $U{SCALE};

select count(*) from PARTSUPP;
ECHOLN BOTH $IF $GTE $LAST[1] $* 800000 $U{SCALE} "PASSED" "***FAILED" ": PARTSUPP table is filled properly. Should be at least " $* 800000 $U{SCALE};

select count(*) from LINEITEM;
ECHOLN BOTH $IF $GTE $LAST[1] $* 6000000 $U{SCALE} "PASSED" "***FAILED" ": LINEITEM table is filled properly. Should be at least " $* 6000000 $U{SCALE};

select count(*) from ORDERS;
ECHOLN BOTH $IF $GTE $LAST[1] $* 1500000 $U{SCALE} "PASSED" "***FAILED" ": ORDERS table is filled properly. Should be at least " $* 1500000 $U{SCALE};




