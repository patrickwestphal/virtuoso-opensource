

create table p_dist (p iri_id_8 primary key (column), cnt bigint);

create table p_cand (c_p iri_id_8 primary key);

log_enable (2);
insert  into p_dist select p, count (*) from rdf_quad group by p;


insert into p_cand select p from (select top 1000 p from p_dist order by cnt desc) ps where rdf_p_stat (p)[0] / rdf_p_stat (p)[1] between 0.7 and 1.4;


create table cset_cand (cc_ps varchar primary key, cnt bigint)

insert into cset_cand select pvec, count (*) from (select s, serialize (vector_agg (p)) as pvec from rdf_quad table option (index rdf_quad_sp, index_only), p_cand where c_p = p group by s option (order, hash)) ps group by pvec;

select dbg_obj_print (cnt, deserialize (cc_ps)) from cset_cand;

CREATE FUNCTION dbg_iri_vec_to_str(in vec any) {
    vec := deserialize(vec);
    declare _len, _ctr integer;
    declare res varchar;
    res := '|';
    _len := length (vec);
    res := concat(res, _len, ' | ');
    _ctr := 0;
    while (_ctr < _len) {
        if (_ctr > 0)
            res := concat(res, ', ');
        res := concat(res, __id2i( vec[_ctr]));
        _ctr := _ctr+1;
    }

    return res;
}

CREATE TABLE tmp_row_number (nr int);
INSERT INTO tmp_row_number VALUES(1);

CREATE FUNCTION create_cset_tbls(in vec any) {
    declare len, ctr integer;
    declare create_tbl_str, tmp_col_name varchar;
    declare tmp_col_name_len integer;
    declare tbl_counter, new_tbl_counter integer;

    for SELECT TOP 1 nr FROM tmp_row_number
    do tbl_counter := nr;
    new_tbl_counter := tbl_counter+1;

    DELETE FROM tmp_row_number;
    INSERT INTO tmp_row_number VALUES(new_tbl_counter);

    vec := deserialize(vec);
    len := length(vec);
    ctr := 0;
    create_tbl_str := concat('CREATE TABLE cset', tbl_counter, ' (s iri_id_8, ');
    -- TODO: consider graphs

    while (ctr < len) {
        -- column name
        tmp_col_name := concat('' , vec[ctr]);
        tmp_col_name_len := length(tmp_col_name);
        create_tbl_str := concat(create_tbl_str, ' tbl', substring(tmp_col_name, 2, tmp_col_name_len-1), ' ');
        -- column type
        create_tbl_str := concat(create_tbl_str, ' ANY');

        ctr := ctr+1;
        -- trailing comma or closing parens
        if (ctr < len) {
            create_tbl_str := concat(create_tbl_str, ', ');
        } else {
            create_tbl_str := concat(create_tbl_str, ');');
        }
    }

    -- looked for something like eval(create_tbl_str) but didn't find anything
    return create_tbl_str;
}

SELECT dbg_iri_vec_to_str(cc_ps), cnt FROM cset_cand WHERE cnt > 10 AND length(deserialize(cc_ps))> 1;

SELECT create_cset_tbls(cc_ps) FROM cset_cand WHERE cnt > 10 AND length(deserialize(cc_ps))> 1;
