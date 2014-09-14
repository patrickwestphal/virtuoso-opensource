
--drop table RESULTS;
create table RESULTS (query integer, run integer, result float, stream integer, rec_time timestamp, start_time datetime, end_time datetime, 
  pre_ru any, post_ru any, pre_mpc bigint, post_mpc bigint,
primary key (query, run, stream));
alter index results  on results partition (query int (0hexffff));

create procedure start_q (in _seq integer, in _q_no integer, in _stream integer)
{
  insert into results (query, run, stream, start_time, pre_ru, pre_mpc) 
    values (_q_no, _seq, _stream, curdatetime (), getrusage (), cl_sys_stat ('mp_mmap_clocks'));
};

create procedure end_q (in _seq integer, in _q_no integer, in _stream integer)
{
  declare et datetime;
  et := curdatetime ();
  update results set end_time = et, result = datediff ('microsecond', START_TIME, et) / 1e3, post_ru = getrusage (), post_mpc = cl_sys_stat ('mp_mmap_clocks') 
  where query = _q_no and run = _seq and stream = _stream;
}; 

create procedure prepare_qs (in _seq integer, in _q_no integer, in _stream integer, in n_cli int := 0)
{
   declare _q_text, _q_name, prepare_15, final_15 any;
   declare split, len any;
   _q_text := file_to_string ('dbgen/qset_' || cast (_stream as varchar) || '_' || cast (_q_no as varchar) || '.sql');
   split := sql_split_text (_q_text);
   len := length (split);
   _q_name := atoi (split[len-1]);
   _q_text :=  split[0];
  _q_text := replace (_q_text, 'select', sprintf ('SELECT /*Q%d-%d-%d */\n',  _q_name, _stream, _seq));
   prepare_15 := final_15 := '';
   if (_q_name = 15) 
     {
       _q_text := split[1];
       prepare_15 := '\n' || split[0] || ';\n';
       final_15 := '\n' || split[2] || ';\n';
     }
   string_to_file ('dbgen/qset_' || cast (_stream as varchar) || '_' || cast (_q_no as varchar) || '.sql', 
       sprintf ('-- %d\nstart_q(%d,%d,%d);\n%s%s;%s\nend_q(%d,%d,%d);\n', 
       _q_name, _seq, _q_name, _stream, prepare_15, _q_text, final_15, _seq, _q_name, _stream), -2);
}
;

create procedure rf_metric (in _seq integer, in _q_no integer, in _stream integer, in n_cli int := 0)
{
   declare _q_text, _q_name, _b_time, _e_time, resul_time, prepare_15, final_15, nth any;
   declare stat, msg, data, meta any;

   nth :=  ((_seq - 1) * (n_cli + 1)) + _stream + 1; -- for rf1 & rf2
   if (_q_no = 23) 
     {
       start_q (_seq, _q_no, _stream);
       resul_time := RF1 ('dbgen', nth); 
       end_q (_seq, _q_no, _stream);
       return;
     };
   if (_q_no = 24) 
     { 
       start_q (_seq, _q_no, _stream);
       resul_time := RF2 ('dbgen', nth); 
       end_q (_seq, _q_no, _stream);
       return;
     };
}
;

create procedure rf1 (in dir varchar, in nth int, in no_pk int := 0, in rb int := 0, in qp int := null)
{
  declare _b_time any;
  _b_time := msec_time();
  if (qp is not null)
    __dbf_set ('enable_qp', qp);
  --log_message (sprintf ('%s/orders.tbl.u%d', dir, nth));
  insert into orders select * from orders_f table option (from sprintf ('%s/orders.tbl.u%d', dir, nth));
  if (no_pk)
    insert into lineitem index lineitem select * from lineitem_f table option (from sprintf ('%s/lineitem.tbl.u%d', dir, nth));
  else
    insert into lineitem select * from lineitem_f table option (from sprintf ('%s/lineitem.tbl.u%d', dir, nth));
  if (rb)
    rollback work;
  else
    commit work;
  return msec_time() - _b_time;
}
;


create procedure del_batch (in d_orderkey int)
{
  vectored;
  delete from lineitem where l_orderkey = d_orderkey;
  delete from orders where o_orderkey = d_orderkey;
}
;

create procedure rf2 (in dir varchar, in nth int)
{
  declare cnt int;
  declare _b_time any;
  _b_time := msec_time();
  cnt := (select count (del_batch (d_orderkey)) from delete_f table option (from sprintf ('%s/delete.%d', dir, nth)));
  commit work;
  return msec_time() - _b_time;
}
;

create procedure power_size (in _scale integer, in seq int)
{
   declare t_sum, res float;

   t_sum := 0.0;

   for (select result from RESULTS where stream = 0 and run = seq) do
     t_sum := t_sum + log (cast (result as float) / 1000);

   res := 3600.0 * exp ((-1.0) * t_sum / 24.0) * cast (_scale as float);

   registry_set (sprintf ('__POWER_SIZE__%d', seq), cast (res as varchar));

   return res;
}
;

create procedure print_power_test_results (in _scale integer, in _run int)
{
   declare ps float;
   declare report any;

   ps  := cast (registry_get (sprintf ('__POWER_SIZE__%d', _run)) as float);

   report := 'Scale:' || cast (_scale as varchar) || '\n\nPower Test results:\n';

-- report := report || 'RF1:' || cast (ri1 as varchar) || ' sec.\n';

   for (select query, result from RESULTS where stream = 0 and run = _run order by query) do
	report := report || 'Q' || cast (query as varchar) || ' - ' ||
		cast ((cast (result as float) / 1000.0) as varchar) || ' sec.\n';


-- report := report || 'RF2:' || cast (ri2 as varchar) || ' sec.\n\n';
   report := report || '\nVirt-H POWER: ' || cast (ps as varchar) || ' \n\n';

   string_to_file ('power_test_results.txt', report, -2);
}
;

create procedure stream (in _num integer, in _all_count integer, in all_streams integer)
{
   declare q_num integer;

   -- MAIN LOOP
   for (q_num := _num; q_num <= _all_count; q_num := q_num + all_streams)
	{
	     log_message ('PASSED: Start ' || cast (q_num as varchar) || ' in ' || cast (_num as varchar) || ' stream.');
   	     for (declare x any, x := 1; x < 25; x := x + 1)
		{
--		  log_message ('+' || cast (x as varchar) || ' in ' || cast (_num as varchar) || ' stream.');
		  metric (q_num, x, _num);
		}
--	     log_message ('Finish: ' || cast (q_num as varchar));
	}
   log_message ('PASSED: Finish stream: ' || cast (_num as varchar));
}
;


create procedure check_results (in _all_count integer, in all_streams integer)
{
   declare q_num, s, total_count, expected_count integer;

   expected_count := (_all_count + 1) * 24;
   select count (*) into total_count from RESULTS;
   log_message (sprintf ('PASSED: CHECK RESULTS LOOPS = %i', _all_count));
   log_message (sprintf ('PASSED: CHECK RESULTS STREAMS = %i', all_streams));
   log_message (sprintf ('PASSED: CHECK RESULTS EXPECTED RESULTS = %i', expected_count));
   log_message (sprintf ('PASSED: CHECK RESULTS CURRENT  RESULTS = %i', total_count));
   -- MAIN LOOP
   for (s := 1; s <= all_streams; s := s + 1)
	{
	  for (q_num := s; q_num <= _all_count; q_num := q_num + all_streams)
	    {
	      for (declare x any, x := 1; x < 23; x := x + 1)
		{
		  if (not exists (select 1 from RESULTS where run = q_num and query = x and s = stream))
		    {
		      log_message (sprintf ('***ABORTED: %i %i %i ', q_num, x, s));
		    }
		}
	    }
	}
   log_message ('PASSED: CHECK RESULTS');
}
;

create procedure get_log ()
{
   declare _file, _line any;
   declare text varchar;

   result_names (text);

   _file := file_rlo ('virtuoso.log');

   while (1)
     {
	_line := file_rl (_file, 1);
	if (length (_line) = 0) goto _finish;
	result (_line[0]);
     }

   end_result ();
_finish:;
}
;

create procedure print_stream_results (in scale integer, in streams integer, in seq integer)
{
   declare _all, report, throughput, query_per_hour, _load_res any;
   declare t1, t2, t3 any;

   _all := file_to_string ('run.output');
   _all := split_and_decode (_all, 0, '\0\0\n');

   if (file_stat ('load.output') = 0)
     _load_res := null;
   else  
     {
       _load_res := file_to_string ('load.output');
       _load_res := split_and_decode (_load_res, 0, '\0\0\n');
     }

   report := 'Virt-H Executive Summary\n\n';
   report := report || sprintf ('Report Date:\t %s %i, %i\n\n', monthname (now()), dayofmonth (now()), year (now()));
   report := report || sprintf ('Database Scale Factor:\t %i\n\n', scale);

   report := report || sprintf ('Total Data Storage/Database Size:\t %iM\n\n',
	sys_stat ('st_db_pages') * sys_stat ('st_db_page_size') / 1024 / 1024);

   if (_load_res is not null)
     {     
       t1 := get_start_load (1, _load_res);
       t2 := get_start_load (2, _load_res);
       t3 := duration (t1, t2);
     }
   else
     {
       t1 := t2 := t3 := '?';
     }

   report := report || sprintf ('Start of Database Load:\t %s\n', t1);
   report := report || sprintf ('End of Database Load:\t %s\n', t2);
   report := report || sprintf ('Database Load Time:\t %s\n\n', t3);

   report := report || sprintf ('Query Streams for Throughput Test:\t %i\n\n', streams);

   report := report || sprintf ('Virt-H Power:\t\t %s\n', f_res_big (cast (registry_get (sprintf ('__POWER_SIZE__%d', seq)) as float)));
   calculate_measurement_interval (seq);
   throughput := (cast (streams as float) * 22.0 * 3600.0 * cast (scale as float))
		/ ((cast (registry_get ('__measurement_interval__') as float) - 50.0) / 1000.0);
   report := report || sprintf ('Virt-H Throughput:\t %s\n', f_res_big (throughput));
   query_per_hour := sqrt (cast (registry_get (sprintf ('__POWER_SIZE__%d', seq)) as float) * throughput);
   report := report || sprintf ('Virt-H Composite Query-per-Hour Metric (Qph@%iGB):\t %s\n\n',
		scale, f_res_big (query_per_hour));

   report := report || sprintf ('Total System Price Over 3 Years:\t\t %s\n', 'Base ?');
   report := report || sprintf ('Virt-H Price Performance Metric (\$/Qph@%iGB):\t %s\n\n', scale, 'Base ?');
   report := report || sprintf ('Measurement Interval:\nMeasurement Interval in Throughput Test (Ts) =\t %s seconds\n\n',
		 f_res (registry_get ('__measurement_interval__')));

   report := report || sprintf ('Duration of stream execution:\n\t\tStart Date/Time\t\tEnd Date/Time\t\tDuration\n');
   for (declare x any, x := 0; x <= streams; x := x + 1)
	{
	   t1 := get_start_end (1, x, _all, seq);
	   t2 := get_start_end (2, x, _all, seq);
	   t3 := duration (t1, t2);

   	   report := report || sprintf ('Stream %i\t%s\t%s\t%s\n', x, t1, t2, t3);
	}

--   if (streams = loops)
     {
	t1 := ((select result from RESULTS where query = 23 and stream = 0 and run = seq));
	t2 := ((select rec_time from RESULTS where query = 23 and stream = 0 and run = seq));
	t1 := datestring (dateadd ('second', (-1) * (t1 + 500) / 1000, t2));
	t2 := datestring (t2);
	t3 := duration (t1, t2);
        report := report || sprintf ('Refresh 0\t%s\t%s\t%s\n', format_rf (t1), format_rf (t2), t3);

	t1 := ((select result from RESULTS where query = 24 and stream = 0 and run = seq));
	t2 := ((select rec_time from RESULTS where query = 24 and stream = 0 and run = seq));
	t1 := datestring (dateadd ('second', (-1) * (t1 + 500) / 1000, t2));
	t2 := datestring (t2);
	t3 := duration (t1, t2);
        report := report || sprintf ('         \t%s\t%s\t%s\n', format_rf (t1), format_rf (t2), t3);
       for (declare x any, x := 1; x <= streams; x := x + 1)
	{
	   t1 := ((select result from RESULTS where query = 23 and stream = x and run = seq));
	   t2 := ((select rec_time from RESULTS where query = 23 and stream = x and run = seq));
	   t3 := ((select rec_time from RESULTS where query = 24 and stream = x and run = seq));
	   t1 := datestring (dateadd ('second', (-1) * (t1 + 500) / 1000, t2));
	   t2 := datestring (t3);
	   t3 := duration (t1, t2);
   	   report := report || sprintf ('Refresh %i\t%s\t%s\t%s\n', x, format_rf (t1), format_rf (t2), t3);
	} 
     }

   normalize_results ();

   report := report || '\nNumerical Quantities Summary Timing Intervals in Seconds:\n';
   report := report || '\nQuery\t\t\t  Q1\t\t  Q2\t\t  Q3\t\t  Q4\t\t  Q5\t\t  Q6\t\t  Q7\t\t  Q8\n';

--   if (streams = loops)
   for (declare x any, x := 0; x <= streams; x := x + 1)
	{
	   report := report || sprintf ('Stream %i\t', x);
	   for (declare q any, q := 1; q <= 8; q := q + 1)
	       report := report || sprintf ('%s\t', f_res ((select result from RESULTS where query = q and stream = x and run = seq)));
    	   report := report || '\n';
	}

    report := report || 'Min Qi\t\t';
    for (declare q any, q := 1; q <= 8; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select min (result) from RESULTS where query = q and stream > 0 and run = seq)));

    report := report || '\nMax Qi\t\t';
    for (declare q any, q := 1; q <= 8; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select max (result) from RESULTS where query = q and stream > 0 and run = seq)));

    report := report || '\nAvg Qi\t\t';
    for (declare q any, q := 1; q <= 8; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select avg (result) from RESULTS where query = q and stream > 0 and run = seq)));


   report := report || '\n\nQuery\t\t\t  Q9\t\t Q10\t\t Q11\t\t Q12\t\t Q13\t\t Q14\t\t Q15\t\t Q16\n';
--   if (streams = loops)
   for (declare x any, x := 0; x <= streams; x := x + 1)
	{
	   report := report || sprintf ('Stream %i\t', x);
	   for (declare q any, q := 9; q <= 16; q := q + 1)
	       report := report || sprintf ('%s\t', f_res ((select result from RESULTS where query = q and stream = x and run = seq)));
    	   report := report || '\n';
	}

    report := report || 'Min Qi\t\t';
    for (declare q any, q := 9; q <= 16; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select min (result) from RESULTS where query = q and stream > 0 and run = seq)));

    report := report || '\nMax Qi\t\t';
    for (declare q any, q := 9; q <= 16; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select max (result) from RESULTS where query = q and stream > 0 and run = seq)));

    report := report || '\nAvg Qi\t\t';
    for (declare q any, q := 9; q <= 16; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select avg (result) from RESULTS where query = q and stream > 0 and run = seq)));


   report := report || '\n\nQuery\t\t\t Q17\t\t Q18\t\t Q19\t\t Q20\t\t Q21\t\t Q22\t\t RF1\t\t RF2\n';
--   if (streams = loops)
   for (declare x any, x := 0; x <= streams; x := x + 1)
	{
	   report := report || sprintf ('Stream %i\t', x);
	   for (declare q any, q := 17; q <= 24; q := q + 1)
	       report := report || sprintf ('%s\t', f_res ((select result from RESULTS where query = q and stream = x and run = seq)));
    	   report := report || '\n';
	}

    report := report || 'Min Qi\t\t';
    for (declare q any, q := 17; q <= 24; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select min (result) from RESULTS where query = q and stream > 0 and run = seq)));

    report := report || '\nMax Qi\t\t';
    for (declare q any, q := 17; q <= 24; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select max (result) from RESULTS where query = q and stream > 0 and run = seq)));

    report := report || '\nAvg Qi\t\t';
    for (declare q any, q := 17; q <= 24; q := q + 1)
        report := report || sprintf ('%s\t', f_res ((select avg (result) from RESULTS where query = q and stream > 0 and run = seq)));


   report := report || '\n';
   string_to_file (sprintf ('report%d.txt', seq), report, -2);
}
;

create procedure normalize_results () -- 5.4.1.4 in spec.
{
  declare _min, _max, _new float;

  for (declare q any, q := 1; q <= 24; q := q + 1)
     {
	_min := ((select min (result) from RESULTS where query = q and stream > 0));
	_max := ((select max (result) from RESULTS where query = q and stream > 0));

	if (_max > (1000 * _min))
	  {
		_new := _max / 1000;
		update RESULTS set result = _new where query = q and stream > 0 and result < _new;
		commit work;
	  }
     }

}
;

create procedure f_res (in _num any)
{
   declare res any;
   res := sprintf ('  %f', (cast (_num as float) + 50)  / 1000);
   --res := "LEFT" (res, strstr (res, '.') + 2);
   --res := "RIGHT" (res, 6);
   return res;
}
;

create procedure f_res_big (in _num any)
{
   declare res any;
   res := sprintf ('      %f', (cast (_num as float) + 0.5));
   res := "LEFT" (res, strstr (res, '.') + 2);
   res := "RIGHT" (res, 11);
   return res;
}
;

create procedure format_rf (in d any)
{
   declare res, p1, p2 any;
   p1 := subseq (d, 5, 10);
   p2 := subseq (d, 11, 19);
   p1 := replace (p1, '-', '/');
   res := sprintf ('%s/%i %s', p1, year (stringdate (d)), p2);
   return res;
}
;

create procedure calculate_measurement_interval (in seq int)
{
   declare t1, t2, t3 any;
   t2 := ((select min (rec_time) from RESULTS where stream > 0 and run = seq));
   t3 := ((select max (rec_time) from RESULTS where stream > 0 and run = seq));
   t1 := ((select result from RESULTS where stream > 0 and rec_time = t2 and run  = seq));
   t1 := dateadd ('second', (-1) * (t1 + 500) / 1000, t2);
   t1 := datediff ('millisecond', t1, t3);
-- The value can be changed after normalize.
-- The registry_set is use only for future use.
   registry_set ('__measurement_interval__', cast (t1 as varchar));

}
;


create procedure print_duration (in _num integer)
{

   declare h, m, s any;

   h := _num / 3600;
   m := (_num - h * 3600) / 60;
   s := (_num - h * 3600 - m * 60);

   m := "RIGHT" (sprintf ('00%i', m), 2);
   s := "RIGHT" (sprintf ('00%i', s), 2);

   return sprintf ('%i:%s:%s', h, m, s);
}
;

create procedure duration (in _t1 any, in _t2 any)
{
   if (_t1 = '--- NONE ---') return '--- NONE ---';
   if (_t2 = '--- NONE ---') return '--- NONE ---';

   _t1 := stringdate (_t1);
   _t2 := stringdate (_t2);
   _t1 := datediff ('second', _t1, _t2);

   return print_duration (_t1);
}
;

create procedure get_start_load (in _mode integer, in _all any)
{
   declare _what any;

  if (_mode = 1)
    _what := 'LOADING TPC-H DATA ';
  else
    _what := 'FINISHED LOADING TPC-H DATA';

   for (declare x any, x := 0; x < length (_all); x := x + 1)
      {
	declare _line any;

	if (strstr (_all[x], _what) > 0)
	  {
	     _line := split_and_decode (_all[x], 0, '\0\0 ');
	     return _line[5 + _mode] || ' ' || _line [6 + _mode];
	  }
      }

   return '--- NONE --- ';
}
;

create procedure get_start_end (in _mode integer, in _num integer, in _all any, in seq int)
{
   declare _what any;

  if (_mode = 1)
    _what := sprintf ('PASSED: START STREAM %d RUN #%d', _num, seq);
  else
    _what := sprintf ('PASSED: FINISH STREAM %d RUN #%d', _num, seq);

   for (declare x any, x := 0; x < length (_all); x := x + 1)
      {
	declare _line any;

	if (strstr (_all[x], _what) = 0)
	  {
	     _line := split_and_decode (_all[x], 0, '\0\0 ');
	     return _line[8] || ' ' || _line [9];
	  }
      }

   return '--- NONE ---';
}
;

