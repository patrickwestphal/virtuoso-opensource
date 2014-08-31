set echo on;



create table T_BITS (TB_ROW_NO int primary key, TB_COMMENT varchar, TB_BITS varchar);
alter index BITS_IDX on DB.DBA.T_BITS partition (TB_ROW_NO int);

insert into sys_vt_index (VI_TABLE,   VI_INDEX,   VI_COL,    VI_ID_COL,   VI_INDEX_TABLE, VI_ID_IS_PK,  VI_OPTIONS)
     values ('DB.DBA.T_BITS', 'T_BITS', 'TB_BITS', 'TB_ROW_NO', 'DB.DBA.T_BITS', 1, serialize (vector ('index', 'bits', 'file', 'bitsfile')));

__ddl_changed ('DB.DBA.T_BITS');

create trigger bins after insert on DB.DBA.T_BITS  
{
  vectored;
  iext_op ('DB.DBA.T_BITS', 'TB_BITS', 'bits', 0, TB_ROW_NO, TB_BITS);
}


create trigger bdel after delete on DB.DBA.T_BITS
{
  vectored;
  iext_op ('DB.DBA.T_BITS', 'TB_BITS', 'bits', 1, TB_ROW_NO, TB_BITS);
}


create trigger bupd after update (TB_ROW_NO, TB_BITS) on DB.DBA.T_BITS referencing old as o
{
  vectored;
  iext_op ('DB.DBA.T_BITS', 'TB_BITS', 'bits', 1, o.TB_ROW_NO, o.TB_BITS);
  iext_op ('DB.DBA.T_BITS', 'TB_BITS', 'bits', 0, TB_ROW_NO, TB_BITS);
}


create procedure rnd_bits (in n int, in mx int)
{
  declare c int;
  declare s any;
 s := string_output ();
  for (c := 0; c < n; c := c + 1)
    http (sprintf ('%d ', rnd (mx)), s);
  return string_output_string (s);
}

create table t1seq (I integer not null primary key);

create procedure t1seq_fill (in imax integer := 1000)
{
  declare ictr integer;
  for (ictr := 0; ictr < imax; ictr := ictr + 1)
    {
      insert soft t1seq (I) values (ictr);
    }
  commit work;
}
;

t1seq_fill ();

insert into DB.DBA.T_BITS (TB_ROW_NO, TB_COMMENT, TB_BITS) select I, sprintf ('row %d', I), rnd_bits (10, 100 + I - I) from t1seq;


select count (*), count (TB_COMMENT) from DB.DBA.T_BITS where contains (TB_BITS, '44', 'index',  'bits');






ttlp ('<xx> <xx> "1234"^^<bits> .', '', 'xx');

insert into sys_vt_index (VI_TABLE, VI_INDEX, VI_COL,  VI_ID_COL,  VI_INDEX_TABLE, VI_ID_IS_PK, VI_OPTIONS)
values ('DB.DBA.RDF_OBJ', 'RDF_OBJ',  'RO_VAL', 'RO_ID', 'DB.DBA.RDF_OBJ', 1,  
serialize (vector ('index', 'bits', 'rdf', (select rdt_twobyte from rdf_datatype where rdt_qname = 'bits'),
'file', 'rdf_bitsfile')));

__ddl_changed ('DB.DBA.RDF_OBJ');




ttlp ('<bits1> <bits> "1 2 3 44 100"^^<bits> . 
  <bits2> <bits> "1 2 3 44 200"^^<bits> . 
  <bits3> <bits> "1 2 3 44 300"^^<bits> . 
  <bits4> <bits> "1 2 3 44 400"^^<bits> .
  <bits4> <bits> "2 3 4 400" .',
 '', 'bits'); 


sparql select * where { ?s <bits> ?b . ?b bif:contains "2" option ("index", "bits")};

