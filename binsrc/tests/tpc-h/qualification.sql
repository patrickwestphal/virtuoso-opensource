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

DROP TABLE QUAL_OUT_Q1;

CREATE TABLE QUAL_OUT_Q1(
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

----INSERT INTO QUAL_OUT_Q1(L_RETURNFLAG,L_LINESTATUS,SUM_QTY,SUM_BASE_PRICE,SUM_DISC_PRICE,SUM_CHARGE,AVG_QTY,AVG_PRICE,AVG_DISC,COUNT_ORDER) VALUES('A','F',371985,521476722,495330564,515060018,25.483661,35725,0.05013,14597);


DROP TABLE QUAL_OUT_Q2;

CREATE TABLE QUAL_OUT_Q2(
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

----INSERT INTO QUAL_OUT_Q2(S_ACCTBAL,S_NAME,N_NAME,P_PARTKEY,P_MFGR,S_ADDRESS,S_PHONE,S_COMMENT) VALUES(6907,'Supplier#2               ','FRANCE                   ',1878,'Manufacturer#5           ','I4sC8pD5tI','16-935-413-6865','dogged somas must kindle quietly besides the tithes --final depths should maintain quiet');


DROP TABLE QUAL_OUT_Q3;

CREATE TABLE QUAL_OUT_Q3(
	L_ORDERKEY	INTEGER,
	REVENUE		NUMERIC,
	O_ORDERDATE	DATE,
	O_SHIPPRIORITY	INTEGER
)
;

----INSERT INTO QUAL_OUT_Q3(L_ORDERKEY,REVENUE,O_ORDERDATE,O_SHIPPRIORITY) VALUES(58915,327519.81,stringdate('1995.03.06 00:00.00 000000'),0);

DROP TABLE QUAL_OUT_Q4;

CREATE TABLE QUAL_OUT_Q4(
	O_ORDERPRIORITY	VARCHAR(15),
	ORDER_COUNT 	INTEGER
)
;

--INSERT INTO QUAL_OUT_Q4(O_ORDERPRIORITY,ORDER_COUNT) VALUES('1-URGENT       ',112);

DROP TABLE QUAL_OUT_Q5;

CREATE TABLE QUAL_OUT_Q5(
	N_NAME		VARCHAR(25),
	REVENUE		NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q5(N_NAME,REVENUE) VALUES('JAPAN                    ',761816.93);

DROP TABLE QUAL_OUT_Q6;

CREATE TABLE QUAL_OUT_Q6(
	REVENUE		NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q6(REVENUE) VALUES(1192025.3);

DROP TABLE QUAL_OUT_Q7;

CREATE TABLE QUAL_OUT_Q7(
	SUPP_NATION	VARCHAR(25),
	CUST_NATION	VARCHAR(25),
	L_YEAR		INTEGER,
	REVENUE		NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q7(SUPP_NATION,CUST_NATION,L_YEAR,REVENUE) VALUES('FRANCE                   ','GERMANY                  ',1995,490085.44);

DROP TABLE QUAL_OUT_Q8;

CREATE TABLE QUAL_OUT_Q8(
	O_YEAR		INTEGER,
	MKT_SHARE	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q8(O_YEAR,MKT_SHARE) VALUES(1995,0);


DROP TABLE QUAL_OUT_Q9;

CREATE TABLE QUAL_OUT_Q9(
	NATION		VARCHAR(25),
	O_YEAR		INTEGER,
	SUM_PROFIT	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q9(NATION,O_YEAR,SUM_PROFIT) VALUES('ALGERIA                  ',1998,158858.22);

DROP TABLE QUAL_OUT_Q10;

CREATE TABLE QUAL_OUT_Q10(
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

--INSERT INTO QUAL_OUT_Q10(C_CUSTKEY,C_NAME,REVENUE,C_ACCTBAL,N_NAME,C_ADDRESS,C_PHONE,C_COMMENT) VALUES(13,'Customer#13              ',491211.51,-57.49,'JAPAN                    ','F2bX8sL3sU2tB7iF8pP3aI2                 ','22-698-962-1230','furious decoys can are sometimes in the theodolites !blithe foxes wil                                                ');

DROP TABLE QUAL_OUT_Q11;

CREATE TABLE QUAL_OUT_Q11(
	PS_PARTKEY	INTEGER,
	VALUE		NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q11(PS_PARTKEY,VALUE) VALUES(995,12197508);

DROP TABLE QUAL_OUT_Q12;

CREATE TABLE QUAL_OUT_Q12(
	L_SHIPMODE	VARCHAR(10),
	HIGH_LINE_COUNT NUMERIC,
	LOW_LINE_COUNT	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q12(L_SHIPMODE,HIGH_LINE_COUNT,LOW_LINE_COUNT) VALUES('MAIL      ',62,95);
--INSERT INTO QUAL_OUT_Q12(L_SHIPMODE,HIGH_LINE_COUNT,LOW_LINE_COUNT) VALUES('SHIP      ',55,92);

DROP TABLE QUAL_OUT_Q13;

CREATE TABLE QUAL_OUT_Q13(
	C_COUNT		NUMERIC,
	CUSTDIST	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q13(C_COUNT,CUSTDIST) VALUES(0,500);

DROP TABLE QUAL_OUT_Q14;

CREATE TABLE QUAL_OUT_Q14(
	PROMO_REVENUE	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q14(PROMO_REVENUE) VALUES(15.1104);

DROP TABLE QUAL_OUT_Q15;

CREATE TABLE QUAL_OUT_Q15(
	S_SUPPKEY	INTEGER,
	S_NAME		VARCHAR(25),
	S_ADDRESS	VARCHAR(40),
	S_PHONE		VARCHAR(15),
	TOTAL_REVENUE	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q15(S_SUPPKEY,S_NAME,S_ADDRESS,S_PHONE,TOTAL_REVENUE) VALUES(15,'Supplier#15              ','V6zB9vU:jI9vQ7nS:aT9tT2pS:','22-384-589-2784',1444981.78);

DROP TABLE QUAL_OUT_Q16;

CREATE TABLE QUAL_OUT_Q16(
	P_BRAND		VARCHAR(10),
	P_TYPE		VARCHAR(25),
	P_SIZE		INTEGER,
	SUPPLIER_CNT	INTEGER
)
;

--INSERT INTO QUAL_OUT_Q16(P_BRAND,P_TYPE,P_SIZE,SUPPLIER_CNT) VALUES('Brand#31  ','SMALL BURNISHED STEEL    ',3 ,8);

DROP TABLE QUAL_OUT_Q17;

CREATE TABLE QUAL_OUT_Q17(
	AVG_YEARLY	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q17(AVG_YEARLY) VALUES(9872.7143);

DROP TABLE QUAL_OUT_Q18;

CREATE TABLE QUAL_OUT_Q18(
	C_NAME		VARCHAR(25),
	C_CUSTKEY	INTEGER,
	O_ORDERKEY	INTEGER,
	O_ORDERDATE	DATE,
	O_TOTALPRICE	NUMERIC,
	SUM_L_QUANTITY	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q18(C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE,SUM_L_QUANTITY) VALUES('Customer#1481            ',1481,44356,stringdate('1996.06.29 00:00.00 000000'),450731.37,293);

DROP TABLE QUAL_OUT_Q19;

CREATE TABLE QUAL_OUT_Q19(
	REVENUE		NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q19(REVENUE) VALUES(4930.38);

DROP TABLE QUAL_OUT_Q20;

CREATE TABLE QUAL_OUT_Q20(
	S_NAME		VARCHAR(25),
	S_ADDRESS	VARCHAR(40)
)
;

--INSERT INTO QUAL_OUT_Q20(S_NAME,S_ADDRESS) VALUES('Supplier#12              ','S1kZ2zS2tI2bM:w');

DROP TABLE QUAL_OUT_Q21;

CREATE TABLE QUAL_OUT_Q21(
	S_NAME		VARCHAR(25),
	NUMWAIT		INTEGER
)
;

--INSERT INTO QUAL_OUT_Q21(S_NAME,NUMWAIT) VALUES('Supplier#4               ',14);
DROP TABLE QUAL_OUT_Q22;


CREATE TABLE QUAL_OUT_Q22(
	CNTRYCODE	VARCHAR(2),
	NUMCUST		INTEGER,
	TOTACCTBAL	NUMERIC
)
;

--INSERT INTO QUAL_OUT_Q22(CNTRYCODE,NUMCUST,TOTACCTBAL) VALUES('13',11,87376.34);
create procedure load_tpch_qual_result (in basepath varchar, in path varchar, in file varchar, in table_name varchar)
{
  declare fpath varchar;
  declare opts any;
  fpath := concat (basepath, '/', path, '/', file);
  opts := vector ('csv-delimiter', ';', 'csv-quote', '"', 'log', 1);
  --csv_load_file( fpath, 1, null, table_name, 2, opts);
  csv_vec_load (file_to_string_output (fpath), 1, null, table_name, 2, opts);
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

load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q1.out',  'QUAL_OUT_Q1');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q2.out',  'QUAL_OUT_Q2');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q3.out',  'QUAL_OUT_Q3');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q4.out',  'QUAL_OUT_Q4');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q5.out',  'QUAL_OUT_Q5');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q6.out',  'QUAL_OUT_Q6');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q7.out',  'QUAL_OUT_Q7');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q8.out',  'QUAL_OUT_Q8');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q9.out',  'QUAL_OUT_Q9');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q10.out', 'QUAL_OUT_Q10');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q11.out', 'QUAL_OUT_Q11');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q12.out', 'QUAL_OUT_Q12');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q13.out', 'QUAL_OUT_Q13');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q14.out', 'QUAL_OUT_Q14');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q15.out', 'QUAL_OUT_Q15');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q16.out', 'QUAL_OUT_Q16');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q17.out', 'QUAL_OUT_Q17');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q18.out', 'QUAL_OUT_Q18');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q19.out', 'QUAL_OUT_Q19');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q20.out', 'QUAL_OUT_Q20');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q21.out', 'QUAL_OUT_Q21');
load_tpch_qual_result('$U{VIRTUOSO_HOME}','binsrc/tests/tpc-h/tpch_answers', 'q22.out', 'QUAL_OUT_Q22');

select case check_csv_loading('QUAL_OUT_Q', 22) when 0 then 'OK' else 'ERROR' end query_results;


create procedure cmp(in tbl1 varchar, in tbl2 varchar)
{
-- The precision of all values contained in the query validation output data must adhere to the following rules:
--a) For singleton column values and results from COUNT aggregates, the values must exactly match the query
--  validation output data.
--b) For ratios, results r must be within 1% of the query validation output data v when rounded to the nearest
--  1/100th. That is, 0.99*v<=round(r,2)<=1.01*v.
--c) For results from SUM aggregates, the resulting values must be within $100 of the query validation output
--  data.
--d) For results from AVG aggregates, the resulting values r must be within 1% of the query validation output
--  data when rounded to the nearest 1/100th. That is, 0.99*v<=round(r,2)<=1.01*v.
--Comment 1: In cases where validation output data is computed using a combination of SUM aggregate and ratios
--(e.g. queries 8,14 and 17), the precision for this validation output data must adhere to bullets b) and c) above.
--Comment 2: In cases where validation output data resembles a row count operation by summing up 0 and 1 using a
--SUM aggregate (e.g. query 12), the precision for this validation output data must adhere to bullet a) above.
--Comment 3: In cases were validation output data is selected from views without any further computation (e.g. total
--revenue in Query 15), the precision for this validation output data must adhere to bullet c) above.
--Comment 4: In cases where validation output data is from the aggregate SUM(l_quantity) (e.g. queries 1 and 18),
--the precision for this validation output data must exactly match the query validation data

   declare meta any;
   declare res1, res2 any;
   declare res_tb1, res_tb2 any;
   declare column1 any;
   declare column2 any;
   declare val1 any;
   declare val2 any;
   declare state, msg, err varchar;
   declare col_name1, col_name2 varchar;
   declare stm1, stm2 varchar;
   declare col, n_cols_1, n_rows_1 integer;
   declare row, n_cols_2, n_rows_2 integer;
   declare col_type1, col_type2 integer;

   tbl1 := complete_table_name (tbl1, 1);
   tbl2 := complete_table_name (tbl2, 1);

   exec (concat ('select \\COLUMN, COL_DTP from SYS_COLS where \\TABLE = \'', tbl1, '\' ORDER BY \\COLUMN'), state, msg, vector (), 100, meta, res1);
   exec (concat ('select \\COLUMN, COL_DTP from SYS_COLS where \\TABLE = \'', tbl2, '\' ORDER BY \\COLUMN'), state, msg, vector (), 100, meta, res2);

   if (length (res1) <> length (res2))
     {
       signal ('00001', sprintf('Tables %s and %s have different number of columns %d and %d', tbl1, tbl2, length (res1), length (res2) ), 'TBL_C');
       return 0;
     }

   n_cols_1 := length (res1);

   for (col := 0; col < n_cols_1; col := col + 1)
     {
        col_name1 := aref (aref (res1, col), 0);
        col_name2 := aref (aref (res2, col), 0);
        col_type1 := aref (aref (res1, col), 1);
        col_type2 := aref (aref (res1, col), 1);

        if (col_name1 <> col_name2)
          return 0;

        if (col_name1 = '_IDN')
          goto next;

        exec (concat ('select ', col_name1, ' from ', tbl1), state, msg, vector (), 100, meta, res_tb1);
        exec (concat ('select ', col_name2, ' from ', tbl2), state, msg, vector (), 100, meta, res_tb2);

        if (length (res_tb1) <> length (res_tb2))
          {
            signal ('00002', sprintf ('Tables %s and %s have different number of rows, %d and %d', tbl1, tbl2, length (res_tb1), length (res_tb2)), 'TBL_C' );
            return 0;
          }

        n_rows_1 := length (res_tb1);

        for (row := 0; row < n_rows_1; row := row + 1)
          {
             val1 := aref (aref (res_tb1, row), 0);
             val2 := aref (aref (res_tb2, row), 0);

             if (__tag (val1) = 181  or __tag (val2) = 181 or __tag (val1) = 182 or __tag (val2) = 182)
               {
                  val1 := trim (val1);
                  val2 := trim (val2);
               }

             if (__tag (val1) <> __tag (val2))
               {
                  if ((__tag (val1) = 181 or __tag (val1) = 182) and (__tag (val2) = 191))
                    {
                      val1 := cast (val1 as integer);
                      val2 := floor (val2);
                    }

                  if ((__tag (val1) = 184) and (__tag (val2) = 181 or __tag (val2) = 191))
                    {
                      val1 := floor (val1);
                      val2 := cast (val2 as integer);
                    }
               }

             if (__tag (val1) = 219)
               {
--                cast (((val1 + 0.5) * 100) as integer) / 100;
--                cast (((val2 + 0.5) * 100) as integer) / 100;
                if(abs( floor (val1 + 0.5) - floor (val2 + 0.5)) >= 100)
                  {
                    signal ('00003', sprintf ('Tables %s and %s have numeric values %s and %s with difference more then 100 in row %d: %s != %s', tbl1, tbl2, col_name1, col_name2, row, cast (val1 as varchar), cast (val2 as varchar)), 'TBL_C');
                    return 0;
                  }
               }

             if (__tag (val1) <> 219 and val1 <> val2)
               {
                  signal ('00004', sprintf ('Tables %s and %s have different values %s and %s in row %d: %s != %s', tbl1, tbl2, col_name1, col_name2, row, cast (val1 as varchar), cast (val2 as varchar)), 'TBL_C');
                  return 0;
               }
          }
next:
        ;
     }
--
-- all is OK.
--
  return 1;
}
;



