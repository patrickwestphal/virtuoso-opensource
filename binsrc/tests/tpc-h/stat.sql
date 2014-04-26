
-- Run result analysis 

-- compare first and second power test 
select a.query, a.result, b.result, a.result - b.result from results a, results b where a.query = b.query and a.stream = 0 and b.stream = 0 and a.run = 1 and b.run = 2  order by 4 desc;

-- cpu percent in power test 
select query, result, post_ru[0] - pre_ru[0] as ucpu, post_ru[1] - pre_ru[1] as scpu, ((post_ru[0] - pre_ru[0]) + (post_ru[1] - pre_ru[1])) / result as pct from results where stream = 0 and run = 2 order by pct;


-- memory allocation cost 
select top 10 query, run, stream, idn ( post_mpc) - idn (pre_mpc) as mpc from results order by idn (post_mpc) - idn (pre_mpc) desc;


create procedure q_idv (in tx varchar)
{
  declare qn varchar;
qn := regexp_substr ('--Q[0-9]*-[0-9]*-[0-9]', cast (tx as varchar), 0);
  if (qn is not null)
    return sprintf_inverse (qn, '--Q%d-%d-%d', 0);
}



create procedure q_id (in tx varchar)
{
  declare qn varchar;
  return  regexp_substr ('--Q[0-9]*-[0-9]*-[0-9]', cast (tx as varchar), 0);
}



