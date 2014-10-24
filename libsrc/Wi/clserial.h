/*
 *
 *  Struct serialization templates for cluster
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2014 OpenLink Software
 *
 *  This project is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; only version 2 of the License, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */


#ifdef SER_WRITE

#define SER(dtp) \
  void cl_serialize_##dtp (dk_session_t * out, dtp * s) \
  {

#define SER_BOX(dtp, tag)				\
  SER (dtp)

#define END_SER }

#define S_BLOB(m) cl_send_blob (out, s->m, s, inx)
#define S_BOX(m) print_object (s->m, out, NULL, NULL)
#define S_CALL(dtp, m) \
  cl_serialize_##dtp ((out), s->m)

#define S_INT(m) print_int (s->m, out)
#define S_FLOAT(m) print_raw_float (s->m, out)
#define S_CHAR(m) session_buffered_write_char (s->m, out)

#else

#undef S_QN_HEAD
#undef SER
#undef SER_BOX
#undef END_SER
#undef S_BLOB
#undef S_BOX
#undef S_CALL
#undef S_INT
#undef S_FLOAT
#undef S_CHAR
#undef S_KEY
#undef S_COL
#undef S_COL_ARRAY
#undef S_TABLE
#undef S_SUBQ
#undef S_QNODE
#undef S_SSL_ARRAY
#undef S_SSL_LIST
#undef S_FULL_SSL_LIST
#undef S_SSL
#undef S_CL_LOC_ARRAY
#undef S_OID_ARRAY
#undef S_FUNC_ARRAY
#undef S_IK_ARRAY
#undef S_COL_PART_ARRAY
#undef S_CODE
#undef S_NODE
#undef S_NODE_REF

#define SER(dtp) \
  dtp * cl_deserialize_##dtp (dk_session_t * in) \
  { \
    NEW_VARZ (dtp, s);


#define SER_BOX(dtp, tag)				 \
  dtp * cl_deserialize_##dtp (dk_session_t * in) \
  { \
    dtp * s = (dtp *) dk_alloc_box_zero (sizeof (dtp), tag);


#define END_SER \
  return s;\
}

#define S_BLOB(m) s->m = cl_receive_blob (in, s, inx)
#define S_BOX(m) s->m = scan_session_boxing (in)
#define S_CALL(dtp, m) \
  s->m = cl_deserialize_##dtp ((in))

#define S_INT(m) s->m = read_boxint (in)
#define S_FLOAT(m) s->m = read_float (in)
#define S_CHAR(m) s->m = session_buffered_read_char (in)

#endif



SER_BOX (cl_op_t, DV_CLOP)
{
  S_CHAR (clo_op);
  S_INT (clo_seq_no);
  S_INT (clo_nth_param_row);
  switch (s->clo_op)
    {
    case CLO_ROW:
      S_BOX (_.row.cols);
      break;
    case CLO_SET_END:
    case CLO_BATCH_END:
    case CLO_AGG_END:
    case CLO_NON_UNQ:
      break;
    case CLO_QF_EXEC:
      S_INT (clo_slice);
      S_INT (_.frag.coord_host);
      if (s->_.frag.coord_host)
	{
	  S_INT (_.frag.nth_stage);
	  S_INT (_.frag.coord_req_no);
	}
      S_INT (_.frag.max_rows);
      S_CHAR (_.frag.isolation);
      S_CHAR (_.frag.lock_mode);
      S_CHAR (_.frag.is_update_replica);
      S_CHAR (_.frag.is_autocommit);
#ifndef SER_WRITE
      {
	S_BOX (_.frag.params);
      }
#else
      {
	float cost;
	int n_sets = 1;
	state_slot_t **param_ssls;
	int inx;
	if (IS_QF (s->_.frag.qf))
	  {
	    cost = s->_.frag.qf->qf_cost;
	    param_ssls = s->_.frag.qf->qf_params;
	  }
	else
	  {
	    stage_node_t *stn = (stage_node_t *) s->_.frag.qf;
	    cost = stn->stn_cost;
	    param_ssls = stn->stn_params;
	  }
	if (cost * n_sets * compiler_unit_msecs * 1000 > qp_thread_min_usec)
	  s->_.frag.merits_thread = 1;
	if (!s->_.frag.params)
	  break;
	dks_array_head (out, BOX_ELEMENTS (param_ssls), DV_ARRAY_OF_POINTER);
	DO_BOX (state_slot_t *, param, inx, param_ssls)
	{
	  if (SSL_IS_VEC_OR_REF (param))
	    {
	      data_col_t *dc = (data_col_t *) s->_.frag.params[inx];
	      n_sets = dc->dc_n_values;
	      print_object ((caddr_t) dc, out, NULL, NULL);
	    }
	  else
	    print_object (qst_get (s->_.frag.qst, param), out, NULL, NULL);
	}
	END_DO_BOX;
      }
#endif
      break;

    }
}

END_SER
