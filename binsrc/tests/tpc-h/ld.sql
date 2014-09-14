select curdatetime ();

status ('');


log_enable (2); insert into lineitem select * from lineitem_f &
log_enable (2); insert into orders select * from orders_f &
log_enable (2); insert into customer select * from customer_f &
log_enable (2); insert into part select * from part_f &
log_enable (2); insert into partsupp select * from partsupp_f &
log_enable (2); insert into supplier select * from supplier_f &
log_enable (2); insert into nation select * from nation_f &
log_enable (2); insert into region select * from region_f &


wait_for_children;
status ('');

select curdatetime ();
ECHOLN BOTH $LAST[1] "START COUNT";

select count(*) from REGION table option (check) &
select count(*) from NATION table option (check) &
select count(*) from SUPPLIER table option (check) &
select count(*) from CUSTOMER table option (check) &
select count(*) from PART table option (check) &
select count(*) from PARTSUPP  table option (check) &
select count(*) from LINEITEM table option (check) &
select count(*) from ORDERS table option (check) &
cl_exec ('checkpoint') &
wait_for_children;



select curdatetime ();
