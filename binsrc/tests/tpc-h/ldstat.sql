
select count(*) from PART table option (check) &
select count(*) from PARTSUPP  table option (check) &
select count(*) from LINEITEM table option (check) &
select count(*) from ORDERS table option (check) &
wait_for_children;
select count(*) from REGION table option (check) &
select count(*) from NATION table option (check) &
select count(*) from SUPPLIER table option (check) &
select count(*) from CUSTOMER table option (check) &

wait_for_children;
