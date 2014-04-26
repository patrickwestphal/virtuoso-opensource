
select count (*) from customer table option (check, index primary key) &
select count (*) from orders table option (check, index primary key) &
select count (*) from lineitem table option (check, index primary key) &
select count (*) from partsupp table option (check, index primary key) &
select count (*) from part table option (check, index primary key) &
select count (*) from supplier table option (check, index primary key) &

wait_for_children;
