--  
--  $Id$
--  
--  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
--  project.
--  
--  Copyright (C) 1998-2014 OpenLink Software
--  
--  This project is free software; you can redistribute it and/or modify it
--  under the terms of the GNU General Public License as published by the
--  Free Software Foundation; only version 2 of the License, dated June 1991.
--  
--  This program is distributed in the hope that it will be useful, but
--  WITHOUT ANY WARRANTY; without even the implied warranty of
--  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
--  General Public License for more details.
--  
--  You should have received a copy of the GNU General Public License along
--  with this program; if not, write to the Free Software Foundation, Inc.,
--  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
--  
--  

--Uses $U{VIRTUOSO_HOME} variable.

DROP TABLE TPCH_OUT_Q1;

CREATE TABLE TPCH_OUT_Q1(
	L_RETURNFLAG	VARCHAR(1),
	L_LINESTATUS	VARCHAR(1),
	SUM_QTY		NUMERIC,
	SUM_BASE_PRICE	NUMERIC,
	SUM_DISC_PRICE	NUMERIC,
	SUM_CHARGE	NUMERIC,
	AVG_QTY		NUMERIC,
	AVG_PRICE	NUMERIC,
	AVG_DISC	NUMERIC,
	COUNT_ORDER 	INTEGER
)
;

----INSERT INTO TPCH_OUT_Q1(L_RETURNFLAG,L_LINESTATUS,SUM_QTY,SUM_BASE_PRICE,SUM_DISC_PRICE,SUM_CHARGE,AVG_QTY,AVG_PRICE,AVG_DISC,COUNT_ORDER) VALUES('A','F',371985,521476722,495330564,515060018,25.483661,35725,0.05013,14597);


DROP TABLE TPCH_OUT_Q2;

CREATE TABLE TPCH_OUT_Q2(
	S_ACCTBAL	NUMERIC,
	S_NAME		VARCHAR(25),
	N_NAME		VARCHAR(25),
	P_PARTKEY	INTEGER,
	P_MFGR		VARCHAR(25),
	S_ADDRESS	VARCHAR(40),
	S_PHONE		VARCHAR(15),
	S_COMMENT	VARCHAR(101)
)
;

----INSERT INTO TPCH_OUT_Q2(S_ACCTBAL,S_NAME,N_NAME,P_PARTKEY,P_MFGR,S_ADDRESS,S_PHONE,S_COMMENT) VALUES(6907,'Supplier#2               ','FRANCE                   ',1878,'Manufacturer#5           ','I4sC8pD5tI','16-935-413-6865','dogged somas must kindle quietly besides the tithes --final depths should maintain quiet');


DROP TABLE TPCH_OUT_Q3;

CREATE TABLE TPCH_OUT_Q3(
	L_ORDERKEY	INTEGER,
	REVENUE		NUMERIC,
	O_ORDERDATE	DATE,
	O_SHIPPRIORITY	INTEGER
)
;

----INSERT INTO TPCH_OUT_Q3(L_ORDERKEY,REVENUE,O_ORDERDATE,O_SHIPPRIORITY) VALUES(58915,327519.81,stringdate('1995.03.06 00:00.00 000000'),0);

DROP TABLE TPCH_OUT_Q4;

CREATE TABLE TPCH_OUT_Q4(
	O_ORDERPRIORITY	VARCHAR(15),
	ORDER_COUNT 	INTEGER
)
;

--INSERT INTO TPCH_OUT_Q4(O_ORDERPRIORITY,ORDER_COUNT) VALUES('1-URGENT       ',112);

DROP TABLE TPCH_OUT_Q5;

CREATE TABLE TPCH_OUT_Q5(
	N_NAME		VARCHAR(25),
	REVENUE		NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q5(N_NAME,REVENUE) VALUES('JAPAN                    ',761816.93);

DROP TABLE TPCH_OUT_Q6;

CREATE TABLE TPCH_OUT_Q6(
	REVENUE		NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q6(REVENUE) VALUES(1192025.3);

DROP TABLE TPCH_OUT_Q7;

CREATE TABLE TPCH_OUT_Q7(
	SUPP_NATION	VARCHAR(25),
	CUST_NATION	VARCHAR(25),
	L_YEAR		INTEGER,
	REVENUE		NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q7(SUPP_NATION,CUST_NATION,L_YEAR,REVENUE) VALUES('FRANCE                   ','GERMANY                  ',1995,490085.44);

DROP TABLE TPCH_OUT_Q8;

CREATE TABLE TPCH_OUT_Q8(
	O_YEAR		INTEGER,
	MKT_SHARE	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q8(O_YEAR,MKT_SHARE) VALUES(1995,0);


DROP TABLE TPCH_OUT_Q9;

CREATE TABLE TPCH_OUT_Q9(
	NATION		VARCHAR(25),
	O_YEAR		INTEGER,
	SUM_PROFIT	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q9(NATION,O_YEAR,SUM_PROFIT) VALUES('ALGERIA                  ',1998,158858.22);

DROP TABLE TPCH_OUT_Q10;

CREATE TABLE TPCH_OUT_Q10(
	C_CUSTKEY	INTEGER,
	C_NAME		VARCHAR(25),
	REVENUE		NUMERIC,
	C_ACCTBAL	NUMERIC,
	N_NAME		VARCHAR(25),
	C_ADDRESS	VARCHAR(40),
	C_PHONE		VARCHAR(15),
	C_COMMENT	VARCHAR(117)
)
;

--INSERT INTO TPCH_OUT_Q10(C_CUSTKEY,C_NAME,REVENUE,C_ACCTBAL,N_NAME,C_ADDRESS,C_PHONE,C_COMMENT) VALUES(13,'Customer#13              ',491211.51,-57.49,'JAPAN                    ','F2bX8sL3sU2tB7iF8pP3aI2                 ','22-698-962-1230','furious decoys can are sometimes in the theodolites !blithe foxes wil                                                ');

DROP TABLE TPCH_OUT_Q11;

CREATE TABLE TPCH_OUT_Q11(
	PS_PARTKEY	INTEGER,
	VALUE		NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q11(PS_PARTKEY,VALUE) VALUES(995,12197508);

DROP TABLE TPCH_OUT_Q12;

CREATE TABLE TPCH_OUT_Q12(
	L_SHIPMODE	VARCHAR(10),
	HIGH_LINE_COUNT NUMERIC,
	LOW_LINE_COUNT	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q12(L_SHIPMODE,HIGH_LINE_COUNT,LOW_LINE_COUNT) VALUES('MAIL      ',62,95);
--INSERT INTO TPCH_OUT_Q12(L_SHIPMODE,HIGH_LINE_COUNT,LOW_LINE_COUNT) VALUES('SHIP      ',55,92);

DROP TABLE TPCH_OUT_Q13;

CREATE TABLE TPCH_OUT_Q13(
	C_COUNT		NUMERIC,
	CUSTDIST	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q13(C_COUNT,CUSTDIST) VALUES(0,500);

DROP TABLE TPCH_OUT_Q14;

CREATE TABLE TPCH_OUT_Q14(
	PROMO_REVENUE	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q14(PROMO_REVENUE) VALUES(15.1104);

DROP TABLE TPCH_OUT_Q15;

CREATE TABLE TPCH_OUT_Q15(
	S_SUPPKEY	INTEGER,
	S_NAME		VARCHAR(25),
	S_ADDRESS	VARCHAR(40),
	S_PHONE		VARCHAR(15),
	TOTAL_REVENUE	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q15(S_SUPPKEY,S_NAME,S_ADDRESS,S_PHONE,TOTAL_REVENUE) VALUES(15,'Supplier#15              ','V6zB9vU:jI9vQ7nS:aT9tT2pS:','22-384-589-2784',1444981.78);

DROP TABLE TPCH_OUT_Q16;

CREATE TABLE TPCH_OUT_Q16(
	P_BRAND		VARCHAR(10),
	P_TYPE		VARCHAR(25),
	P_SIZE		INTEGER,
	SUPPLIER_CNT	INTEGER
)
;

--INSERT INTO TPCH_OUT_Q16(P_BRAND,P_TYPE,P_SIZE,SUPPLIER_CNT) VALUES('Brand#31  ','SMALL BURNISHED STEEL    ',3 ,8);

DROP TABLE TPCH_OUT_Q17;

CREATE TABLE TPCH_OUT_Q17(
	AVG_YEARLY	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q17(AVG_YEARLY) VALUES(9872.7143);

DROP TABLE TPCH_OUT_Q18;

CREATE TABLE TPCH_OUT_Q18(
	C_NAME		VARCHAR(25),
	C_CUSTKEY	INTEGER,
	O_ORDERKEY	INTEGER,
	O_ORDERDATE	DATE,
	O_TOTALPRICE	NUMERIC,
	SUM_L_QUANTITY	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q18(C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE,SUM_L_QUANTITY) VALUES('Customer#1481            ',1481,44356,stringdate('1996.06.29 00:00.00 000000'),450731.37,293);

DROP TABLE TPCH_OUT_Q19;

CREATE TABLE TPCH_OUT_Q19(
	REVENUE		NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q19(REVENUE) VALUES(4930.38);

DROP TABLE TPCH_OUT_Q20;

CREATE TABLE TPCH_OUT_Q20(
	S_NAME		VARCHAR(25),
	S_ADDRESS	VARCHAR(40)
)
;

--INSERT INTO TPCH_OUT_Q20(S_NAME,S_ADDRESS) VALUES('Supplier#12              ','S1kZ2zS2tI2bM:w');

DROP TABLE TPCH_OUT_Q21;

CREATE TABLE TPCH_OUT_Q21(
	S_NAME		VARCHAR(25),
	NUMWAIT		INTEGER
)
;

--INSERT INTO TPCH_OUT_Q21(S_NAME,NUMWAIT) VALUES('Supplier#4               ',14);
DROP TABLE TPCH_OUT_Q22;


CREATE TABLE TPCH_OUT_Q22(
	CNTRYCODE	VARCHAR(2),
	NUMCUST		INTEGER,
	TOTACCTBAL	NUMERIC
)
;

--INSERT INTO TPCH_OUT_Q22(CNTRYCODE,NUMCUST,TOTACCTBAL) VALUES('13',11,87376.34);

create procedure load_query_result (in basepath varchar, in path varchar, in file varchar, in table_name varchar)
{
  declare fpath varchar;
  declare opts any;
  declare s,r any;
  fpath := concat (basepath, '/', path, '/', file);
  opts := vector ('csv-delimiter', ',', 'csv-quote', '\'', 'log', 1);
  -- ' for srupid syntax highlighters.
  csv_vec_load (file_to_string_output (fpath), 0, null, table_name, 2, opts);
  --csv_load_file( fpath, 1, null, table_name, 2, opts);
  
  --s := file_open (fpath);
  --while (isvector (r := get_csv_row (s, ',', '\'', null, null)))
  --return csv_load (s, _from, _to, tb, log_mode, opts);
  --}
}
;

create procedure check_csv_loading (in tab_prefix varchar, in ntables int)
{
  declare i, cnt int;
  declare stm varchar;
  declare state, msg, descs, rows any;
  state := '00000';
  for (i := 1; i <= ntables; i:= i + 1)
  {
     stm := sprintf( 'select count(*) from %s%d', tab_prefix, i);
     exec (stm, state, msg, vector (), 1, descs, rows);

     if (state <> '00000')
       signal (state, msg);

     if (length (rows) = 0 or aref(aref(rows,0),0) = 0)
       return 1;
  }
  return 0;
}
;

load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q1.out',  'TPCH_OUT_Q1');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q2.out',  'TPCH_OUT_Q2');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q3.out',  'TPCH_OUT_Q3');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q4.out',  'TPCH_OUT_Q4');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q5.out',  'TPCH_OUT_Q5');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q6.out',  'TPCH_OUT_Q6');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q7.out',  'TPCH_OUT_Q7');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q8.out',  'TPCH_OUT_Q8');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q9.out',  'TPCH_OUT_Q9');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q10.out', 'TPCH_OUT_Q10');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q11.out', 'TPCH_OUT_Q11');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q12.out', 'TPCH_OUT_Q12');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q13.out', 'TPCH_OUT_Q13');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q14.out', 'TPCH_OUT_Q14');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q15.out', 'TPCH_OUT_Q15');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q16.out', 'TPCH_OUT_Q16');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q17.out', 'TPCH_OUT_Q17');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q18.out', 'TPCH_OUT_Q18');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q19.out', 'TPCH_OUT_Q19');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q20.out', 'TPCH_OUT_Q20');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q21.out', 'TPCH_OUT_Q21');
load_query_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/data', 'q22.out', 'TPCH_OUT_Q22');

select case check_csv_loading('TPCH_OUT_Q', 22) when 0 then 'OK' else 'ERROR' end loading;

create procedure check_query_results (in qual_tab_prefix varchar, in run_tab_prefix varchar, in ntables int)
{
  -- cmp(in tbl1 varchar, in tbl2 varchar)
  declare res, i, cnt int;
  declare stm varchar;
  declare state, msg, descs, rows any;
  state := '00000';
  res := 0;
  for (i := 1; i <= ntables; i:= i + 1)
  { 
     whenever sqlstate '*' goto do_continue;
     stm := sprintf( 'select cmp( \'%s\', \'%s\' )', qual_tab_prefix, run_tab_prefix, i);
     exec (stm, state, msg, vector (), 1, descs, rows);

     if (state <> '00000')
     {
       signal (state, msg);
       res := 1;
     }

     if (length (rows) = 0 or aref(aref(rows,0),0) <> 0)
     {
       signal ('00042', sprintf( 'TPCH Q%d results are different.', i) );
       res := 1;
     }
do_continue:
     ;
  }
  return res;
}
;

--select case check_query_results('QUAL_OUT_Q', 'TPCH_OUT_Q', 22) when 0 then 'OK' else 'ERROR' end as query_results;

select case cmp('QUAL_OUT_Q1', 'TPCH_OUT_Q1') when 1 then 'ok' else 'DIFFERENT' end as 'Q1';
select case cmp('QUAL_OUT_Q2', 'TPCH_OUT_Q2') when 1 then 'ok' else 'DIFFERENT' end as 'Q2';
select case cmp('QUAL_OUT_Q3', 'TPCH_OUT_Q3') when 1 then 'ok' else 'DIFFERENT' end as 'Q3';
select case cmp('QUAL_OUT_Q4', 'TPCH_OUT_Q4') when 1 then 'ok' else 'DIFFERENT' end as 'Q4';
select case cmp('QUAL_OUT_Q5', 'TPCH_OUT_Q5') when 1 then 'ok' else 'DIFFERENT' end as 'Q5';
select case cmp('QUAL_OUT_Q6', 'TPCH_OUT_Q6') when 1 then 'ok' else 'DIFFERENT' end as 'Q6';
select case cmp('QUAL_OUT_Q7', 'TPCH_OUT_Q7') when 1 then 'ok' else 'DIFFERENT' end as 'Q7';
select case cmp('QUAL_OUT_Q8', 'TPCH_OUT_Q8') when 1 then 'ok' else 'DIFFERENT' end as 'Q8';
select case cmp('QUAL_OUT_Q9', 'TPCH_OUT_Q9') when 1 then 'ok' else 'DIFFERENT' end as 'Q9';
select case cmp('QUAL_OUT_Q10', 'TPCH_OUT_Q10') when 1 then 'ok' else 'DIFFERENT' end as 'Q10';
select case cmp('QUAL_OUT_Q11', 'TPCH_OUT_Q11') when 1 then 'ok' else 'DIFFERENT' end as 'Q11';
select case cmp('QUAL_OUT_Q12', 'TPCH_OUT_Q12') when 1 then 'ok' else 'DIFFERENT' end as 'Q12';
select case cmp('QUAL_OUT_Q13', 'TPCH_OUT_Q13') when 1 then 'ok' else 'DIFFERENT' end as 'Q13';
select case cmp('QUAL_OUT_Q14', 'TPCH_OUT_Q14') when 1 then 'ok' else 'DIFFERENT' end as 'Q14';
select case cmp('QUAL_OUT_Q15', 'TPCH_OUT_Q15') when 1 then 'ok' else 'DIFFERENT' end as 'Q15';
select case cmp('QUAL_OUT_Q16', 'TPCH_OUT_Q16') when 1 then 'ok' else 'DIFFERENT' end as 'Q16';
select case cmp('QUAL_OUT_Q17', 'TPCH_OUT_Q17') when 1 then 'ok' else 'DIFFERENT' end as 'Q17';
select case cmp('QUAL_OUT_Q18', 'TPCH_OUT_Q18') when 1 then 'ok' else 'DIFFERENT' end as 'Q18';
select case cmp('QUAL_OUT_Q19', 'TPCH_OUT_Q19') when 1 then 'ok' else 'DIFFERENT' end as 'Q19';
select case cmp('QUAL_OUT_Q20', 'TPCH_OUT_Q20') when 1 then 'ok' else 'DIFFERENT' end as 'Q20';
select case cmp('QUAL_OUT_Q21', 'TPCH_OUT_Q21') when 1 then 'ok' else 'DIFFERENT' end as 'Q21';
select case cmp('QUAL_OUT_Q22', 'TPCH_OUT_Q22') when 1 then 'ok' else 'DIFFERENT' end as 'Q22';


-- select (select count(*) from PART)*9 + (select count(*) from PARTSUPP)*5 + (select count(*) from LINEITEM)*16 + (select count(*) from ORDERS)*9 + (select count(*) from CUSTOMER)*8 + (select count(*) from SUPPLIER)*7 + (select count(*) from NATION)*4 + (select count(*) from REGION)*3

