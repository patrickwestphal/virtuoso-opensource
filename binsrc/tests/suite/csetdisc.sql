

create table p_dist (p iri_id_8 primary key (column), cnt bigint);

create table p_cand (c_p iri_id_8 primary key);

log_enable (2);
insert  into p_dist select p, count (*) from rdf_quad group by p;


insert into p_cand select p from (select top 1000 p from p_dist order by cnt desc) ps where rdf_p_stat (p)[0] / rdf_p_stat (p)[1] between 0.7 and 1.4;


create table cset_cand (cc_ps varchar primary key, cnt bigint)

insert into cset_cand select pvec, count (*) from (select s, serialize (vector_agg (p)) as pvec from rdf_quad table option (index rdf_quad_sp, index_only), p_cand where c_p = p group by s option (order, hash)) ps group by pvec;

select dbg_obj_print (cnt, deserialize (cc_ps)) from cset_cand;

