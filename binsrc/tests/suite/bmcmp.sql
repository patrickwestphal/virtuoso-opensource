


create table FEATURE (
  F_ID bigint,
  f_slide int,
  f_x int,
  f_y int,
  f_geo any,
  f_P1 int,
  F_P2 int,
  f_bitmap long varchar,
  primary key (f_id) column);

create column index f_slide on feature (f_slide);


create table F_RTREE (X real no compress, Y real no compress,X2 real no compress, Y2 real no compress, ID bigint no compress, primary key (X, Y, X2, Y2, ID) not column);

insert into sys_vt_index (vi_table, vi_index, vi_col, vi_id_col, vi_index_table, vi_id_is_pk, vi_options)
  values ('DB.DBA.FEATURE', 'FEATURE', 'F_GEO', 'F_ID', 'DB.DBA.F_RTREE', 1, 'G');




create procedure init_cmp (in slide_no int)
{
  declare cnt int;
  log_enable (0, 1);
  delete  from  f_rtree;
  commit work;
  set non_txn_insert on;
  log_enable (2, 1);
  select count (geo_insert ('DB.DBA.F_RTREE', f_geo, f_id)) into cnt from feature where f_slide = slide_no;
  return cnt;
}


create procedure rnd_string (in l int)
{
  declare s any;
  declare c int;
 s := make_string (l);
  for (c := 0; c < l; c := c + 1)
    s[c] := rnd (256);
  return s;
}


create procedure rnd_slide (in sid int, in xmax int, in ymax int, in n_features int, in fsize int)
{
  declare ctr, x, y, xn, yn, xq, yq int;
  declare bits, geo any;
  set non_txn_insert = 1;
  log_enable (2, 1);
 xn := xmax / fsize;
 yn := ymax / fsize;
 bits := make_array (n_features, 'any');
 geo := make_array (n_features, 'any');
  for (ctr := 0; ctr < n_features; ctr := ctr + 1)
    {
    x := fsize * rnd (xn);
    y := fsize * rnd (yn);
    bits[ctr] := rnd_string (fsize * fsize / 8);
    geo[ctr] := st_geomfromtext (sprintf ('SRID=0;BOX2D(%d %d, %d %d)', x, y, x + fsize, y + fsize));
    };
  for vectored (in g any := geo, in b any := bits) {
      insert into feature (f_id, f_slide, f_geo, f_bitmap, f_p1, f_p2, f_x, f_y)
	values (sequence_next ('s_seq'), sid, g, b, rnd (100), rnd (1000), st_xmin (g), st_ymin (g));
    }
}


rnd_slide (11, 10000, 10000, 500000, 70);
rnd_slide (22, 10000, 10000, 500000, 70);


init_cmp (22);



select count (*) from feature f1, feature f2 where 
  f1.f_slide = 11 and f2.f_slide = 22 and st_intersects (f2.f_geo, f1.f_geo) option (order);


select max (bm_cmp (f1.f_x, f1.f_y, f2.f_x, f2.f_y, f1.f_bitmap, f2.f_bitmap))
from feature f1, feature f2 where 
  f1.f_slide = 11 and f2.f_slide = 22 and st_intersects (f2.f_geo, f1.f_geo) option (order);




select f1.f_id, f2.f_id, bm_cmp (f1.f_x, f1.f_y, f2.f_x, f2.f_y, f1.f_bitmap, f2.f_bitmap) as sc, f1.f_p1, f2.f_p1
  from feature f1, feature f2 where f1.f_slide = 11 and f2.f_slide = 22 and st_intersects (f2.f_geo, f1.f_geo)
option (order);


