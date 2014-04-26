
__dbf_set ('enable_vec', 1);
log_enable (2, 1);
insert into C..LINEITEM index lineitem select * from DB..LINEITEM;
insert into C..LINEITEM index c_l_pk (l_orderkey, l_linenumber, l_partkey) select l_orderkey, l_linenumber, l_partkey from DB..LINEITEM table option (index l_pk);


insert into c..orders index orders select * from db..orders;
insert into c..orders index c_o_ck (o_orderkey, o_custkey) select o_orderkey, o_custkey from db..orders table option (index o_ck);

insert into c..part select * from db..part;
insert into c..partsupp index partsupp select * from db..partsupp;
insert into c..partsupp index c_ps_skpk (ps_partkey, ps_suppkey)  select ps_partkey, ps_suppkey from db..partsupp table option (index ps_skpk);

insert into c..customer index customer select * from db..customer;
insert into c..customer index c_c_nk (c_custkey, c_nationkey) select c_custkey, c_nationkey from db..customer table option (index c_nk);

insert into c..supplier index supplier select * from db..supplier;
insert into c..supplier index c_s_nk (s_suppkey, s_nationkey)  select s_suppkey, s_nationkey from db..supplier table option (index s_nk);


insert into c..region select * from db..region;
insert into c..nation select * from db..nation;


select count (*) from c..lineitem a where  not exists (select 1 from c..lineitem b table option (loop) where b.l_partkey = a.l_partkey and b.l_orderkey = a.l_orderkey and b.l_linenumber = b.l_linenumber);


select top 10 l_partkey, l_orderkey, l_linenumber  from c..lineitem a where 0 + l_partkey > 000 and  not exists (select 1 from c..lineitem b table option (loop) where b.l_partkey = a.l_partkey and b.l_orderkey = a.l_orderkey and b.l_linenumber = a.l_linenumber);

select top 10 l_partkey, l_orderkey, l_linenumber  from c..lineitem a table option (index lineitem) where 0 + l_partkey > 000 and  not exists (select 1 from c..lineitem b table option (loop) where b.l_partkey = a.l_partkey and b.l_orderkey = a.l_orderkey /*and b.l_linenumber = b.l_linenumber */ );


select top 10 o_orderkey  from c..orders a where not exists (select 1 from c..orders b table option (loop) where b.o_custkey = a.o_custkey and b.o_orderkey = a.o_orderkey);;

select top 10 o_orderkey, o_custkey  from c..orders a where not exists (select 1 from c..orders b table option (loop, index orders) where b.o_custkey = a.o_custkey and b.o_orderkey = a.o_orderkey);


select top 10 ps_partkey, ps_suppkey from c..partsupp a table option (index partsupp) where not exists (select 1 from c..partsupp b table option (loop) where a.ps_partkey = b.ps_partkey and a.ps_suppkey = b.ps_suppkey);

select top 10 c_nationkey, c_custkey from c..customer a where not exists (select 1 from c..customer b table option (loop, index customer) where b.c_custkey = a.c_custkey and b.c_nationkey = a.c_nationkey);

select top 10 c_nationkey, c_custkey from c..customer a table option (index c_c_nk) where not exists (select 1 from c..customer b table option (loop, index c_c_nk) where b.c_custkey = a.c_custkey and b.c_nationkey = a.c_nationkey);

select  s_nationkey, count (*) from c..supplier, c..customer where s_suppkey + 0 < 100 and c_nationkey = s_nationkey group by s_nationkey order by s_nationkey option (loop, order)  ;

select sum (l_extendedprice - ps_supplycost) from lineitem, partsupp, part where p_type like '%GREEN%'and l_partkey = p_partkey and ps_partkey = p_partkey and ps_suppkey = l_suppkey;
create table q9r (ok int, pk int, sk int);

insert into q9r
select l_orderkey, l_partkey, l_suppkey from lineitem, part where p_name like '%green%'and l_partkey = p_partkey;

insert into q9c
select l_orderkey, l_partkey, l_suppkey from c..lineitem, part where p_name like '%green%'and l_partkey = p_partkey;
