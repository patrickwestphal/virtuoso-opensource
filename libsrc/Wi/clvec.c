/*
 *  clvec.c
 *
 *  $Id$
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



#include "sqlnode.h"
#include "sqlbif.h"
#include "arith.h"
#include "eqlcomp.h"
#include "sqlfn.h"
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "datesupp.h"

int enable_ksp_fast = 1;


void
dc_serialize (data_col_t * dc, dk_session_t * ses)
{
  int inx;
  int64 bytes_before = ses->dks_bytes_sent + ses->dks_out_fill, bytes_after;
  session_buffered_write_char (DV_DATA, ses);
  if (sizeof (caddr_t) == box_length (dc))
    {
      caddr_t ser = *(caddr_t *) dc;
      int bytes = LONG_REF_NA (ser);
      session_buffered_write (ses, ser, bytes + 12);
      return;
    }
  print_long (0, ses);
  print_long (dc->dc_n_values, ses);
  session_buffered_write_char (dc->dc_dtp, ses);
  session_buffered_write_char (dc->dc_type, ses);
  session_buffered_write_char (dc->dc_sqt.sqt_non_null, ses);
  session_buffered_write_char (dc->dc_any_null, ses);
  if (DCT_BOXES & dc->dc_type)
    {
      for (inx = 0; inx < dc->dc_n_values; inx++)
	print_object (((caddr_t *) dc->dc_values)[inx], ses, NULL, NULL);
    }
  else
    {
      if (DV_ANY != dc->dc_dtp && dc->dc_any_null)
	session_buffered_write (ses, (char *) dc->dc_nulls, ALIGN_8 (dc->dc_n_values) / 8);
      switch (dc->dc_dtp)
	{
	case DV_IRI_ID:
	case DV_IRI_ID_8:
	case DV_LONG_INT:
	case DV_DOUBLE_FLOAT:
	  session_buffered_write (ses, (char *) dc->dc_values, sizeof (int64) * dc->dc_n_values);
	  break;
	case DV_SINGLE_FLOAT:
	  session_buffered_write (ses, (char *) dc->dc_values, sizeof (float) * dc->dc_n_values);
	  break;
	case DV_DATETIME:
	  session_buffered_write (ses, (char *) dc->dc_values, DT_LENGTH * dc->dc_n_values);
	  break;

	case DV_ANY:
	  {
	    for (inx = 0; inx < dc->dc_n_values; inx++)
	      {
		int l;
		db_buf_t dv = ((db_buf_t *) dc->dc_values)[inx];
		DB_BUF_TLEN (l, dv[0], dv);
		session_buffered_write (ses, (char *) dv, l);
	      }
	    break;
	  }
	default:
	  GPF_T1 ("dc of unknown type for serialization");
	}
    }
  bytes_after = ses->dks_bytes_sent + ses->dks_out_fill;
  strses_set_int32 (ses, bytes_before + 1, bytes_after - (bytes_before + 13));
}


data_col_t *
dc_deserialize_in_dc (dk_session_t * ses, data_col_t * dc, int bytes, int n_values)
{
  int inx;
  int elt_sz;
  dtp_t dcdtp, dctype, dcnn, any_null;
  mem_pool_t *mp;
  if (-1 == bytes)
    {
      bytes = read_long (ses);
      n_values = read_long (ses);
    }
  dcdtp = session_buffered_read_char (ses);
  dctype = session_buffered_read_char (ses);
  dcnn = session_buffered_read_char (ses);
  any_null = session_buffered_read_char (ses);
  if (!dc)
    {
      mp = THR_TMP_POOL;
      dc = (data_col_t *) mp_alloc_box (mp, sizeof (data_col_t), DV_DATA);
      memzero (dc, sizeof (data_col_t));
      dc->dc_n_values = n_values;
      dc->dc_n_places = dc->dc_n_values;
      dc->dc_mp = mp;
      dc->dc_dtp = dcdtp;
      dc->dc_type = dctype;
      dc->dc_sqt.sqt_non_null = dcnn;
      dc->dc_any_null = any_null;
      elt_sz = dc_elt_size (dc);
      dc->dc_values = (db_buf_t) mp_alloc_box (mp, 16 + (dc->dc_n_values * elt_sz), DV_NON_BOX);
      dc->dc_values = (db_buf_t) ALIGN_16 ((ptrlong) dc->dc_values);
    }
  else
    {
      dc_reset (dc);
      dc_convert_empty (dc, dcdtp);
      dc->dc_any_null = any_null;
      DC_CHECK_LEN (dc, n_values - 1);
      dc->dc_n_values = n_values;
    }
  if (DCT_BOXES & dc->dc_type)
    {
      dc->dc_n_values = 0;
      for (inx = 0; inx < n_values; inx++)
	{
	  /* deterministic order of read and inc of dc values in case the fuck throws out of here */
	  caddr_t box = scan_session_boxing (ses);
	  ((caddr_t *) dc->dc_values)[dc->dc_n_values++] = box;
	}
      return dc;
    }
  if (dc->dc_any_null && DV_ANY != dc->dc_dtp)
    {
      int nb = ALIGN_8 (dc->dc_n_values) / 8;
      dc_ensure_null_bits (dc);
      session_buffered_read (ses, (char *) dc->dc_nulls, nb);
    }
  switch (dc->dc_dtp)
    {
    case DV_IRI_ID:
    case DV_IRI_ID_8:
    case DV_LONG_INT:
    case DV_DOUBLE_FLOAT:
      session_buffered_read (ses, (char *) dc->dc_values, sizeof (int64) * dc->dc_n_values);
      break;
    case DV_SINGLE_FLOAT:
      session_buffered_read (ses, (char *) dc->dc_values, sizeof (float) * dc->dc_n_values);
      break;
    case DV_DATETIME:
      session_buffered_read (ses, (char *) dc->dc_values, DT_LENGTH * dc->dc_n_values);
      break;

    case DV_ANY:
      {
	int n_values = dc->dc_n_values;
	dc->dc_n_values = 0;
	for (inx = 0; inx < n_values; inx++)
	  {
	    int l;
	    db_buf_t dv = (db_buf_t) ses->dks_in_buffer + ses->dks_in_read;
	    DB_BUF_TLEN (l, dv[0], dv);
	    dc_append_bytes (dc, dv, l, NULL, 0);
	    ses->dks_in_read += l;
	  }
	break;
      }
    default:
      GPF_T1 ("dc of unknown type for serialization");
    }
  return dc;
}


data_col_t *
dc_deserialize (dk_session_t * ses, dtp_t dtp)
{
  cl_ses_t *clses = DKS_CL_DATA (ses);
  uint32 bytes = read_long (ses);
  uint32 n_values = read_long (ses);
  if (n_values > dc_max_batch_sz || bytes < n_values)
    GPF_T1 ("dc recd with more values than max vec or less bytes than values");
  if (!clses || (clses && clses->clses_reading_req_clo))
    {
      /* always just record the place in the cm.  May read dcs only from cluster cm strings */
      caddr_t *box = dk_alloc_box (sizeof (caddr_t), DV_DATA);
      *(caddr_t *) box = ses->dks_in_buffer + ses->dks_in_read - 8;
      ses->dks_in_read += bytes + 4;
      return (data_col_t *) box;
    }
  return dc_deserialize_in_dc (ses, NULL, bytes, n_values);
}


void
dc_qf_xml (data_col_t * dc, query_instance_t * qi)
{
  int inx;
  for (inx = 0; inx < dc->dc_n_values; inx++)
    {
      caddr_t x = ((caddr_t *) dc->dc_values)[inx];
      QF_XML (qi, x);
    }
}


int
qf_param_dcs (query_t * qr, caddr_t * inst, mem_pool_t * mp, caddr_t * params)
{
  int any = 0, inx;
  DO_BOX (caddr_t *, box, inx, params)
  {
    if (DV_DATA == DV_TYPE_OF (box) && sizeof (caddr_t) == box_length (box))
      {
	any = 1;
	break;
      }
  }
  END_DO_BOX;
  if (any)
    {
      int n_sets = -1;
      scheduler_io_data_t iod;
      dk_session_t ses;
      memzero (&ses, sizeof (ses));
      memzero (&iod, sizeof (iod));
      DKS_QI_DATA (&ses) = (QI *) inst;
      ses.dks_in_fill = INT32_MAX;
      SESSION_SCH_DATA ((&ses)) = &iod;
      SET_THR_TMP_POOL (mp);
      CATCH_READ_FAIL (&ses)
      {
	DO_BOX (caddr_t, box, inx, params)
	{
	  if (DV_DATA == DV_TYPE_OF (box) && sizeof (caddr_t) == box_length (box))
	    {
	      state_slot_t *ssl = qr->qr_qf_params[inx];
	      data_col_t *dc = QST_BOX (data_col_t *, inst, ssl->ssl_index);
	      ses.dks_in_buffer = *(caddr_t *) box;
	      ses.dks_in_read = 0;
	      DKS_QI_DATA (&ses) = (QI *) inst;
	      dk_free_box (box);
	      params[inx] = (caddr_t) dc_deserialize_in_dc (&ses, dc, -1, -1);
	      dc = (data_col_t *) params[inx];
	      n_sets = dc->dc_n_values;
	      if (DCT_BOXES & dc->dc_type)
		dc_qf_xml (dc, (query_instance_t *) inst);
	      dc_set_flags (dc, &dc->dc_sqt, ssl->ssl_dc_dtp);
	      inst[ssl->ssl_index] = (caddr_t) dc;
	    }
	}
	END_DO_BOX;
      }
      FAILED n_sets = -1;
      END_READ_FAIL (&ses);
      SET_THR_TMP_POOL (NULL);
      return n_sets;
    }
  return 1;
}


caddr_t
box_deserialize_xml (db_buf_t dv, caddr_t * inst)
{
  scheduler_io_data_t iod;
  dk_session_t ses;
  memset (&ses, 0, sizeof (ses));
  memset (&iod, 0, sizeof (iod));
  ses.dks_in_buffer = (caddr_t) dv;
  ses.dks_in_fill = INT32_MAX;
  SESSION_SCH_DATA ((&ses)) = &iod;
  DKS_QI_DATA (&ses) = (QI *) inst;
  return (caddr_t) read_object (&ses);
}


#define dc_dtp_elt_size(dtp)  \
  (DV_SINGLE_FLOAT == dtp ? 4 : DV_DATETIME == dtp ? DT_LENGTH : 8)

caddr_t
dre_box (dc_read_t * dre, caddr_t reuse, caddr_t * inst)
{
  if (DV_ANY == dre->dre_dtp || (DCT_BOXES & dre->dre_type))
    {
      int l;
      DB_BUF_TLEN (l, dre->dre_data[0], dre->dre_data);
      if (DV_XML_ENTITY == dre->dre_data[0])
	{
	  dk_free_tree (reuse);
	  reuse = box_deserialize_xml (dre->dre_data, inst);
	}
      else
	reuse = box_deserialize_reusing (dre->dre_data, reuse);
      dre->dre_data += l;
      dre->dre_pos++;
      return reuse;
    }
  if (dre->dre_any_null && BIT_IS_SET (dre->dre_nulls, dre->dre_pos))
    {
      if (!IS_BOX_POINTER (reuse) || DV_DB_NULL != box_tag (reuse))
	{
	  dk_free_tree (reuse);
	  reuse = dk_alloc_box (0, DV_DB_NULL);
	}
      dre->dre_pos++;
      dre->dre_data += dc_dtp_elt_size (dre->dre_dtp);
      return reuse;
    }
  dre->dre_pos++;
  switch (dre->dre_dtp)
    {
    case DV_LONG_INT:
    case DV_IRI_ID:
    case DV_IRI_ID_8:
    case DV_DOUBLE_FLOAT:
      if (!IS_BOX_POINTER (reuse) || dre->dre_dtp != box_tag (reuse))
	{
	  dk_free_tree (reuse);
	  reuse = dk_alloc_box (8, dre->dre_dtp);
	}
      *(int64 *) reuse = INT64_REF_CA (dre->dre_data);
      dre->dre_data += 8;
      return reuse;
    case DV_SINGLE_FLOAT:
      if (!IS_BOX_POINTER (reuse) || dre->dre_dtp != box_tag (reuse))
	{
	  dk_free_tree (reuse);
	  reuse = dk_alloc_box (8, dre->dre_dtp);
	}
      *(int32 *) reuse = LONG_REF_CA (dre->dre_data);
      dre->dre_data += 4;
      return reuse;
    case DV_DATETIME:
      if (!IS_BOX_POINTER (reuse) || DV_DATETIME != box_tag (reuse))
	{
	  dk_free_box (reuse);
	  reuse = dk_alloc_box (DT_LENGTH, DV_DATETIME);
	}
      memcpy_dt (reuse, dre->dre_data);
      dre->dre_data += DT_LENGTH;
      return reuse;
    }
  GPF_T;
  return NULL;
}


void
dc_any_trap (db_buf_t dv)
{
  if (DV_DATETIME == dv[0] && 11 == dv[1] && 48 == dv[2] && 37 == dv[3] && 23 == dv[4])
    bing ();
}

void
clib_vec_read_into_slots (cll_in_box_t * clib, caddr_t * inst, dk_set_t slots)
{
  int inx = 0, l;
  int64 i;
  itc_cluster_t *itcl = clib->clib_group ? clib->clib_group->clrg_itcl : clib->clib_itcl;
  int row, n_rows = itcl->itcl_batch_size - itcl->itcl_n_results;
  int n_avail = clib->clib_dc_read[0].dre_n_values - clib->clib_dc_read[0].dre_pos;
  int dc_fill, dre_pos;
  n_rows = MIN (n_rows, n_avail);
  clib->clib_first._.row.cols = NULL;	/* for safety when freeing */
  DO_SET (state_slot_t *, ssl, &slots)
  {
    data_col_t *dc = QST_BOX (data_col_t *, inst, ssl->ssl_index);
    dc_read_t *dre = &clib->clib_dc_read[inx];
    if (!inx)
      {
	dre_pos = dre->dre_pos;
	dc_fill = dc->dc_n_values;
      }
    else if (dre_pos != dre_pos || dc_fill != dc->dc_n_values)
      GPF_T1 ("reading clib into slits, uneven dc length or dre pos");
    if (!itcl->itcl_n_results)
      DC_CHECK_LEN (dc, n_rows - 1);
    if (DV_ANY == ssl->ssl_sqt.sqt_dtp)
      {
	if (0 == dc->dc_n_values && dc->dc_dtp != dre->dre_dtp)
	  dc_convert_empty (dc, dre->dre_dtp);
	if (dc->dc_dtp != dre->dre_dtp && DV_ANY != dc->dc_dtp)
	  dc_heterogenous (dc);
      }
    if (DCT_BOXES & dc->dc_type)
      {
	for (row = 0; row < n_rows; row++)
	  {
	    db_buf_t dv = dre->dre_data;
	    DB_BUF_TLEN (l, *dv, dv);
	    if (DV_DB_NULL == dv[0])
	      dc->dc_any_null = 1;
	    else if (DV_XML_ENTITY == dv[0])
	      ((caddr_t *) dc->dc_values)[dc->dc_n_values++] = box_deserialize_xml (dv, clib->clib_group->clrg_inst);
	    else
	      ((caddr_t *) dc->dc_values)[dc->dc_n_values++] = box_deserialize_string ((caddr_t) dv, l, 0);
	    dre->dre_data += l;
	  }
      }
    else if (DV_ANY == dc->dc_dtp)
      {
	if (DV_ANY == dre->dre_dtp)
	  {
	    for (row = 0; row < n_rows; row++)
	      {
		db_buf_t dv = dre->dre_data;
		DB_BUF_TLEN (l, *dv, dv);
		if (DV_DB_NULL == *dv)
		  dc->dc_any_null = 1;
		/*dc_any_trap  (dv); */
		dc_append_bytes (dc, dv, l, NULL, 0);
		dre->dre_data += l;
	      }
	  }
	else
	  {
	    dtp_t tmp[20];
	    dtp_t nu = DV_DB_NULL;
	    for (row = 0; row < n_rows; row++)
	      {
		if (dre->dre_nulls && BIT_IS_SET (dre->dre_nulls, dre->dre_pos + row))
		  dc_append_bytes (dc, &nu, 1, NULL, 0);
		else
		  {
		    switch (dre->dre_dtp)
		      {
		      case DV_LONG_INT:
			i = INT64_REF_CA (dre->dre_data);
			dv_from_int (tmp, i);
			dre->dre_data += 8;
			dc_append_bytes (dc, tmp, db_buf_const_length[tmp[0]], NULL, 0);
			break;
		      case DV_IRI_ID:
			i = INT64_REF_CA (dre->dre_data);
			dv_from_iri (tmp, i);
			dre->dre_data += 8;
			dc_append_bytes (dc, tmp, db_buf_const_length[tmp[0]], NULL, 0);
			break;
		      case DV_DOUBLE_FLOAT:
			EXT_TO_DOUBLE (tmp, dre->dre_data);
			dc_append_bytes (dc, tmp, 8, &dre->dre_dtp, 1);
			dre->dre_data += 8;
			break;
		      case DV_SINGLE_FLOAT:
			EXT_TO_FLOAT (tmp, dre->dre_data);
			dc_append_bytes (dc, tmp, 4, &dre->dre_dtp, 1);
			dre->dre_data += 4;
			break;
		      case DV_DATETIME:
			dc_append_bytes (dc, dre->dre_data, DT_LENGTH, &dre->dre_dtp, 1);
			dre->dre_data += DT_LENGTH;
			break;
		      default:
			GPF_T1 ("recd unknown typed dc");
		      }
		  }
	      }
	  }
      }
    else
      {
	int elt_sz = dc_dtp_elt_size (dre->dre_dtp);
	int pos = dre->dre_pos;
	if (dtp_canonical[dre->dre_dtp] != dtp_canonical[dc->dc_dtp])
	  {
	    log_error ("receiving dre dtp %d for dc dtp %d ssl %d, indicates bad plan, should report query to support",
		dre->dre_dtp, dc->dc_dtp, ssl->ssl_index);
	    sqlr_new_error ("CLVEC", "MSDTP",
		"receiving dre dtp %d for dc dtp %d ssl %d, indicates bad plan, should report query to support", dre->dre_dtp,
		dc->dc_dtp, ssl->ssl_index);
	  }
	DC_CHECK_LEN (dc, dc->dc_n_values + n_rows - 1);
	memcpy_16 (dc->dc_values + dc->dc_n_values * elt_sz, dre->dre_data, elt_sz * n_rows);
	dre->dre_data += elt_sz * n_rows;
	if (!dre->dre_any_null && dc->dc_nulls)
	  {
	    for (row = 0; row < n_rows; row++)
	      BIT_CLR (dc->dc_nulls, dc->dc_n_values + row);
	  }
	else if (dre->dre_any_null)
	  {
	    if (!dc->dc_nulls)
	      dc_ensure_null_bits (dc);
	    for (row = 0; row < n_rows; row++)
	      {
		if (BIT_IS_SET (dre->dre_nulls, row + pos))
		  {
		    BIT_SET (dc->dc_nulls, dc->dc_n_values + row);
		  }
		else
		  BIT_CLR (dc->dc_nulls, dc->dc_n_values + row);
	      }
	    dc->dc_any_null = 1;
	  }
	dc->dc_n_values += n_rows;
      }
    dre->dre_pos += n_rows;
    inx++;
  }
  END_DO_SET ();
  clib->clib_first._.row.nth_val += n_rows;
  itcl->itcl_n_results += n_rows;
  clib->clib_rows_done += n_rows - 1;	/* caller increments one more time */
}



int clib_trap_qf;
int clib_trap_col;



caddr_t *
clo_qf_params (cl_req_group_t * clrg, data_source_t * qn, int n_rows, dc_val_cast_t * cf)
{
  int inx, n_params;
  caddr_t *inst = clrg->clrg_inst;
  mem_pool_t *mp = clrg->clrg_itcl->itcl_pool;
  state_slot_t **params = NULL;
  key_source_t *ks = NULL;
  data_col_t **dcs;
  if (IS_QN (qn, query_frag_input))
    {
      QNCAST (query_frag_t, qf, qn);
      params = qf->qf_params;
      ks = qf->qf_loc_ts->ts_order_ks;
    }
  else if (IS_QN (qn, stage_node_input))
    {
      QNCAST (stage_node_t, stn, qn);
      params = stn->stn_params;
      ks = stn->stn_loc_ts->ts_order_ks;
    }
  n_params = BOX_ELEMENTS (params);
  dcs = (data_col_t **) mp_alloc_box (mp, n_params * sizeof (caddr_t), DV_BIN);

  DO_BOX (state_slot_t *, ssl, inx, ks->ks_vec_cast)
  {
    if (SSL_VEC == ssl->ssl_type)
      {
	data_col_t *dc = dcs[inx] = mp_data_col (mp, ssl, n_rows);
	data_col_t *source_dc = QST_BOX (data_col_t *, inst, ks->ks_vec_source[inx]->ssl_index);
	if (DV_ANY == dc->dc_dtp && DV_ANY != source_dc->dc_dtp && !cf[inx])
	  dc_convert_empty (dc, source_dc->dc_dtp);
      }
  }
  END_DO_BOX;
  for (inx = inx; inx < n_params; inx++)
    {
      state_slot_t *ssl = params[inx];
      dcs[inx] = (data_col_t *) qst_get (clrg->clrg_itcl->itcl_qst, ssl);
    }
  return (caddr_t *) dcs;
}

cl_op_t *
qn_clo (cl_req_group_t * clrg, data_source_t * qn, int n_rows, cl_op_t * param_clo, dc_val_cast_t * cf)
{
  state_slot_t **params;
  cl_op_t *clo = mp_clo_allocate (clrg->clrg_itcl->itcl_pool, CLO_QF_EXEC);
  if (IS_QN (qn, query_frag_input))
    {
      QNCAST (query_frag_t, qf, qn);
      params = qf->qf_params;
    }
  else if (IS_QN (qn, stage_node_input))
    {
      QNCAST (stage_node_t, stn, qn);
      params = stn->stn_params;
    }
  clo->_.frag.qf = (query_frag_t *) qn;
  clo->_.frag.qst = clrg->clrg_itcl->itcl_qst;
  if (param_clo)
    clo->_.frag.params = param_clo->_.frag.params;
  else
    clo->_.frag.params = clo_qf_params (clrg, qn, n_rows, cf);
  return clo;
}

extern resource_t *clib_rc;


cll_in_box_t *clrg_new_clib (cl_req_group_t * clrg, cl_host_t * host, int csl_id, data_source_t * target, int n_rows);

cl_op_t *
clrg_csl_clo (cl_req_group_t * clrg, data_source_t * qn, cl_host_t * host, cl_slice_t * slice, cl_op_t * org_clo,
    cl_op_t * param_clo, int n_rows, dc_val_cast_t * cf)
{
  cluster_map_t *clm = clrg->clrg_clm;
  cll_in_box_t *clib = CLRG_CSL_CLIB (clrg, slice->csl_id, clm->clm_host_rank[host->ch_id]);
  cl_op_t *clo;
  if (clib)
    {
      clo = clib->clib_vec_clo;
      if (!clib->clib_waiting && !dk_set_member (org_clo->clo_clibs, (void *) clib))
	mp_set_push (clo->clo_pool, &org_clo->clo_clibs, (void *) clib);
      clib->clib_is_active = clib->clib_waiting = 1;
      clo->_.frag.coord_req_no = org_clo->_.frag.coord_req_no;
      clo->_.frag.coord_host = org_clo->_.frag.coord_host;
      return clo;
    }
  clrg->clrg_all_sent = 0;
  clo = qn_clo (clrg, qn, n_rows, param_clo, cf);
  clo->_.frag.coord_host = org_clo->_.frag.coord_host;
  clo->_.frag.nth_stage = org_clo->_.frag.nth_stage;
  clo->_.frag.coord_req_no = org_clo->_.frag.coord_req_no;
  clo->_.frag.qf_id = org_clo->_.frag.qf_id;
  clo->_.frag.isolation = org_clo->_.frag.isolation;
  clo->_.frag.lock_mode = org_clo->_.frag.lock_mode;
  clo->_.frag.is_update = org_clo->_.frag.is_update;
  clo->_.frag.is_autocommit = org_clo->_.frag.is_autocommit;
  clo->_.frag.max_rows = org_clo->_.frag.max_rows;
  clo->clo_seq_no = ~CL_DA_FOLLOWS & (clrg->clrg_clo_seq_no++);
  {
    cll_in_box_t *clib = (cll_in_box_t *) resource_get (clib_rc);
    clib->clib_host = host;
    clib->clib_slice = slice->csl_id;
    mp_set_push (clo->clo_pool, &org_clo->clo_clibs, (void *) clib);
    clib->clib_is_active = clib->clib_waiting = 1;
    clib->clib_group = clrg;
    clib->clib_vectored = 1;
    clib->clib_enlist = clrg->clrg_need_enlist ? CM_ENLIST : 0;
    clrg_add_clib (clrg, clib);
    clib->clib_vec_clo = clo;
    clo->clo_slice = slice->csl_id;
    CLRG_CSL_CLIB (clrg, slice->csl_id, clm->clm_host_rank[host->ch_id]) = clib;
    return clib->clib_vec_clo;
  }
}


void
clrg_target_clm (cl_req_group_t * clrg, cluster_map_t * clm)
{
  mem_pool_t *mp = clrg->clrg_itcl ? clrg->clrg_itcl->itcl_pool : clrg->clrg_pool;
  if (clrg->clrg_clm)
    return;
  clrg->clrg_clm = clm;
  clrg->clrg_is_elastic = clm->clm_is_elastic;
  clrg->clrg_slice_clibs =
      (cll_in_box_t **) mp_alloc_box (mp, clm->clm_distinct_slices * clm->clm_n_replicas * sizeof (caddr_t), DV_BIN);
  memzero (clrg->clrg_slice_clibs, box_length (clrg->clrg_slice_clibs));
}


void
clrg_target_dcs (cl_req_group_t * clrg, cl_op_t * target_clo, int part_cols, state_slot_t ** target_ssl, data_col_t ** target)
{
  int inx, n_params;
  for (inx = 0; inx < part_cols; inx++)
    {
      /* partition cols already cast so now move them from the previous target dcs to the ones determined by the partition. */
      data_col_t *prev_dc = target[inx];
      data_col_t *target_dc = (data_col_t *) target_clo->_.frag.params[inx];
      int elt_sz;
      if (target_dc == prev_dc)
	continue;
      elt_sz = dc_elt_size (prev_dc);
      target[inx] = target_dc;
      prev_dc->dc_n_values--;
      DC_CHECK_LEN (target_dc, target_dc->dc_n_values);
      memcpy (target_dc->dc_values + elt_sz * target_dc->dc_n_values, prev_dc->dc_values + elt_sz * prev_dc->dc_n_values, elt_sz);
      target_dc->dc_n_values++;
    }
  n_params = BOX_ELEMENTS (target_ssl);
  for (inx = inx; inx < n_params; inx++)
    target[inx] = (data_col_t *) target_clo->_.frag.params[inx];
}


uint32
dc_part_hash (col_partition_t * cp, data_col_t * dc, int set, int32 * rem_ret)
{
  uint32 hash;
  if (DV_ANY == dc->dc_dtp)
    return cp_any_hash (cp, ((db_buf_t *) dc->dc_values)[set], rem_ret);
  if (DCT_BOXES & dc->dc_type)
    {
      caddr_t err = NULL;
      caddr_t str;
      AUTO_POOL (100);
      str = box_to_any_1 (((caddr_t *) dc->dc_values)[set], &err, &ap, DKS_TO_DC);
      if (err)
	sqlr_resignal (err);
      hash = cp_any_hash (cp, (db_buf_t) str, rem_ret);
      if (str < ap.ap_area || str > ap.ap_area + ap.ap_fill)
	dk_free_box (str);
      return hash;
    }
  switch (cp->cp_type)
    {
    case CP_INT:
      {
	boxint i = ((int64 *) dc->dc_values)[set];
	hash = cp_int_hash (cp, i, rem_ret);
	return hash;
      }
    case CP_WORD:
      {
	switch (dc->dc_dtp)
	  {
	  case DV_DATETIME:
	    return cp_string_hash (cp, ((caddr_t) dc->dc_values) + DT_LENGTH * (set), DT_COMPARE_LENGTH, rem_ret);
	  case DV_IRI_ID:
	  case DV_LONG_INT:
	    {
	      int64 i = ((int64 *) dc->dc_values)[set];
	      return cp_int_any_hash (cp, i, rem_ret);
	    }
	  case DV_SINGLE_FLOAT:
	    return cp_double_hash (cp, (double) ((float *) dc->dc_values)[set], rem_ret);
	  case DV_DOUBLE_FLOAT:
	    return cp_double_hash (cp, ((double *) dc->dc_values)[set], rem_ret);
	  default:
	    sqlr_new_error ("42000", "CLVEC", "Supposed to be an any dc for partitioning by word");
	  }
      }
    }
  GPF_T1 ("should have decided in dc_part_hash");
  return 0;
}


#define MAX_PARAMS 100


void
qf_clrg_reuse (data_source_t * qn, key_source_t * ks, caddr_t * inst, cl_req_group_t * clrg, dc_val_cast_t * cf)
{
  /* with dfg and aggregation, a single clrg gets to send multiple partitioned batches.  */
  int n_vec = BOX_ELEMENTS (ks->ks_vec_cast);
  state_slot_t **params = IS_QN (qn, query_frag_input) ? ((query_frag_t *) qn)->qf_params : ((stage_node_t *) qn)->stn_params;
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
  {
    clib->clib_waiting = 0;
    clib->clib_part_n_rows = 0;
    clib->clib_dc_start = 0;
    clib->clib_keep_alive = 0;
    strses_flush (clib->clib_req_strses);
    if (clib->clib_vec_clo)
      {
	int inx;
	clib->clib_vec_clo->_.frag.is_started = 0;
	DO_BOX (data_col_t *, dc, inx, clib->clib_vec_clo->_.frag.params)
	{
	  if (inx < n_vec)
	    {
	      dc_reset (dc);
	      if (DV_ANY == params[inx]->ssl_sqt.sqt_dtp && !cf[inx])
		{
		  data_col_t *source_dc = QST_BOX (data_col_t *, inst, ks->ks_vec_source[inx]->ssl_index);
		  if (DV_ANY != source_dc->dc_dtp)
		    dc_convert_empty (dc, source_dc->dc_dtp);
		  if (DV_ANY == source_dc->dc_dtp && DV_ANY != dc->dc_dtp)
		    dc_heterogenous (dc);
		}
	    }
	  else
	    clib->clib_vec_clo->_.frag.params[inx] = qst_get (inst, params[inx]);
	}
	END_DO_BOX;
      }
  }
  END_DO_SET ();
}


void
clib_start_dc (cll_in_box_t * clib, data_col_t * dc, int anify)
{
  dk_session_t *ses = clib->clib_req_strses;
  clib->clib_dc_start = ses->dks_bytes_sent + ses->dks_out_fill;
  session_buffered_write_char (DV_DATA, ses);
  print_long (0, ses);
  print_long (0, ses);
  if (anify)
    {
      session_buffered_write_char (DV_ANY, ses);
      session_buffered_write_char (0, ses);
    }
  else
    {
      session_buffered_write_char (dc->dc_dtp, ses);
      session_buffered_write_char (dc->dc_type, ses);
    }
  session_buffered_write_char (dc->dc_sqt.sqt_non_null, ses);
  session_buffered_write_char (dc->dc_any_null, ses);
}


void
clib_finish_dc (cll_in_box_t * clib)
{
  dk_session_t *ses = clib->clib_req_strses;
  int bytes_before = clib->clib_dc_start;
  int bytes_after = ses->dks_bytes_sent + ses->dks_out_fill;
  strses_set_int32 (ses, bytes_before + 1, bytes_after - (bytes_before + 13));
  strses_set_int32 (ses, bytes_before + 5, clib->clib_part_n_rows);
}


void
clrg_start_dc (cl_req_group_t * clrg, data_col_t * dc, int anify)
{
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs) if (clib->clib_part_n_rows)
    clib_start_dc (clib, dc, anify);
  END_DO_SET ();
}


void
clrg_dc_done (cl_req_group_t * clrg)
{
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs) if (clib->clib_part_n_rows)
    clib_finish_dc (clib);
  END_DO_SET ();
}


void
clrg_partition_dc_anify (cl_req_group_t * clrg, data_col_t * dc, int *set_nos, slice_id_t * slices, int first, int last,
    int nth_param)
{
  dtp_t tmp[20];
  int inx, len;
  dk_session_t *ses;
  data_col_t *target_dc;
  if (DCT_BOXES & dc->dc_type)
    {
      AUTO_POOL (500);
      for (inx = 0; inx < last - first; inx++)
	{
	  caddr_t err = NULL;
	  int set = set_nos ? set_nos[inx] : inx + first;
	  slice_id_t slid = slices[first + inx];
	  cll_in_box_t *clib = CLRG_CSL_CLIB (clrg, slid, 0);
	  caddr_t box = ((caddr_t *) dc->dc_values)[set];
	  caddr_t str = box_to_any_1 (box, &err, &ap, DKS_TO_DC);
	  if (err)
	    sqlr_resignal (err);
	  if (clib->clib_is_local)
	    {
	      target_dc = (data_col_t *) clib->clib_vec_clo->_.frag.params[nth_param];
	      dc_append_bytes (target_dc, (db_buf_t) str, box_length (str) - 1, NULL, 0);
	    }
	  else
	    {
	      ses = clib->clib_req_strses;
	      session_buffered_write (ses, str, box_length (str) - 1);
	    }
	  if (str < ap.ap_area || str > ap.ap_area + ap.ap_fill)
	    dk_free_box (str);
	}
      return;
    }
  switch (dc->dc_dtp)
    {
    case DV_LONG_INT:

#define DC_PART_ANY_HEAD \
      for (inx = 0; inx < last - first; inx++) \
	{ \
	  int set = set_nos ? set_nos[inx] : inx + first; \
	  slice_id_t slid = slices[first + inx]; \
	  cll_in_box_t * clib = CLRG_CSL_CLIB (clrg, slid, 0) \

      DC_PART_ANY_HEAD;
      int64 i = ((int64 *) dc->dc_values)[set];
      dv_from_int (tmp, i);
      len = db_buf_const_length[tmp[0]];

#define DC_PART_ANY_TAIL \
	  if (clib->clib_is_local) \
	    { \
	      target_dc = (data_col_t*)clib->clib_vec_clo->_.frag.params[nth_param]; \
	      dc_append_bytes (target_dc, tmp, len, NULL, 0); \
	      continue; \
	    } \
	  ses = clib->clib_req_strses; \
	  session_buffered_write (ses, (char*)tmp, len); \
	}

      DC_PART_ANY_TAIL;
      break;
    case DV_IRI_ID:
      DC_PART_ANY_HEAD;
      iri_id_t i = ((int64 *) dc->dc_values)[set];
      dv_from_iri (tmp, i);
      len = db_buf_const_length[tmp[0]];

      DC_PART_ANY_TAIL;
      break;

    case DV_SINGLE_FLOAT:
      DC_PART_ANY_HEAD;
      float i = ((float *) dc->dc_values)[set];
      tmp[0] = DV_SINGLE_FLOAT;
      FLOAT_TO_EXT (&tmp[1], &i);
      len = 5;
      DC_PART_ANY_TAIL;
      break;

    case DV_DOUBLE_FLOAT:
      DC_PART_ANY_HEAD;
      double i = ((double *) dc->dc_values)[set];
      tmp[0] = DV_DOUBLE_FLOAT;
      DOUBLE_TO_EXT (&tmp[1], &i);
      len = 9;
      DC_PART_ANY_TAIL;
      break;

    case DV_DATETIME:
      DC_PART_ANY_HEAD;
      dtp_t *i = dc->dc_values + DT_LENGTH * set;
      tmp[0] = DV_DATETIME;
      memcpy_dt (&tmp[1], i);
      len = 1 + DT_LENGTH;
      DC_PART_ANY_TAIL;
      break;
    default:
      GPF_T1 ("unsupported dc dtp in dc partition anify");
    }
}


void
clrg_partition_dc (cl_req_group_t * clrg, data_col_t * dc, int *set_nos, slice_id_t * slices, int first, int last, int nth_param,
    int anify)
{
  int inx;
  int elt_sz;
  dk_session_t *ses;
  data_col_t *target_dc;
  int set;
  slice_id_t slid;
  cll_in_box_t *clib;
  int limit = last - first;
  if (anify)
    {
      clrg_partition_dc_anify (clrg, dc, set_nos, slices, first, last, nth_param);
      return;
    }
  elt_sz = dc_elt_size (dc);
  if ((DCT_NUM_INLINE & dc->dc_type) && 8 == elt_sz)
    {
      int64 *values = (int64 *) dc->dc_values;
      for (inx = 0; inx < limit; inx++)
	{
	  int64 i;
	  if (set_nos)
	    {
	      set = set_nos[inx];
	      if (inx + 8 < limit && 0 == (inx & 3))
		{
		  __builtin_prefetch (&values[set_nos[inx + 4]]);
		  __builtin_prefetch (&values[set_nos[inx + 5]]);
		  __builtin_prefetch (&values[set_nos[inx + 46]]);
		  __builtin_prefetch (&values[set_nos[inx + 7]]);
		}
	    }
	  else
	    set = inx + first;
	  i = values[set];
	  slid = slices[first + inx];
	  clib = CLRG_CSL_CLIB (clrg, slid, 0);
	  if (clib->clib_is_local)
	    {
	      target_dc = (data_col_t *) clib->clib_vec_clo->_.frag.params[nth_param];
	      dc_append_int64 (target_dc, i);
	      continue;
	    }
	  ses = clib->clib_req_strses;
	  if (ses->dks_out_fill + 8 <= ses->dks_out_length)
	    {
	      *(int64 *) & ses->dks_out_buffer[ses->dks_out_fill] = i;
	      ses->dks_out_fill += 8;
	    }
	  else
	    session_buffered_write (ses, (char *) &i, 8);
	}
    }
  else if (DV_SINGLE_FLOAT == dc->dc_dtp)
    {
      for (inx = 0; inx < limit; inx++)
	{
	  int set = set_nos ? set_nos[inx] : inx + first;
	  float i = ((float *) dc->dc_values)[set];
	  slice_id_t slid = slices[first + inx];
	  cll_in_box_t *clib = CLRG_CSL_CLIB (clrg, slid, 0);
	  if (clib->clib_is_local)
	    {
	      dc_append_float ((data_col_t *) clib->clib_vec_clo->_.frag.params[nth_param], i);
	      continue;
	    }
	  ses = clib->clib_req_strses;
	  if (ses->dks_out_fill + 4 <= ses->dks_out_length)
	    {
	      *(float *) &ses->dks_out_buffer[ses->dks_out_fill] = i;
	      ses->dks_out_fill += 4;
	    }
	  else
	    session_buffered_write (ses, (caddr_t) & i, 4);
	}
    }
  else if (DV_DATETIME == dc->dc_dtp)
    {
      for (inx = 0; inx < limit; inx++)
	{
	  int set = set_nos ? set_nos[inx] : inx + first;
	  dtp_t *i = &dc->dc_values[set * DT_LENGTH];
	  slice_id_t slid = slices[first + inx];
	  cll_in_box_t *clib = CLRG_CSL_CLIB (clrg, slid, 0);
	  if (clib->clib_is_local)
	    {
	      target_dc = (data_col_t *) clib->clib_vec_clo->_.frag.params[nth_param];
	      DC_CHECK_LEN (target_dc, target_dc->dc_n_values);
	      memcpy_dt (target_dc->dc_values + target_dc->dc_n_values * DT_LENGTH, i);
	      target_dc->dc_n_values++;
	      continue;
	    }
	  ses = clib->clib_req_strses;
	  session_buffered_write (ses, (char *) i, DT_LENGTH);
	}
    }
  else if (DCT_BOXES & dc->dc_type)
    {
      for (inx = 0; inx < limit; inx++)
	{
	  int set = set_nos ? set_nos[inx] : inx + first;
	  caddr_t i = ((caddr_t *) dc->dc_values)[set];
	  slice_id_t slid = slices[first + inx];
	  cll_in_box_t *clib = CLRG_CSL_CLIB (clrg, slid, 0);
	  if (clib->clib_is_local)
	    {
	      target_dc = (data_col_t *) clib->clib_vec_clo->_.frag.params[nth_param];
	      DC_CHECK_LEN (target_dc, target_dc->dc_n_values);
	      ((caddr_t *) target_dc->dc_values)[target_dc->dc_n_values++] = box_mt_copy_tree (i);
	      /* mt copy, different targets run on different threads, unserialized ref counts do not work so copy rdf boxes etc */
	      continue;
	    }
	  ses = clib->clib_req_strses;
	  print_object (i, ses, NULL, NULL);
	}
    }
  else if (DV_ANY == dc->dc_dtp)
    {
      db_buf_t *values = (db_buf_t *) dc->dc_values;
      int len;
      for (inx = 0; inx < limit; inx++)
	{
	  db_buf_t i;
	  if (set_nos)
	    {
	      set = set_nos[inx];
	      if (inx + 8 < limit && 0 == (inx & 3))
		{
		  __builtin_prefetch (values[set_nos[inx + 4]]);
		  __builtin_prefetch (values[set_nos[inx + 5]]);
		  __builtin_prefetch (values[set_nos[inx + 6]]);
		  __builtin_prefetch (values[set_nos[inx + 7]]);
		}
	    }
	  else
	    set = inx + first;
	  i = values[set];
	  slid = slices[first + inx];
	  clib = CLRG_CSL_CLIB (clrg, slid, 0);
	  DB_BUF_TLEN (len, i[0], i);
	  if (clib->clib_is_local)
	    {
	      dc_append_bytes ((data_col_t *) clib->clib_vec_clo->_.frag.params[nth_param], i, len, NULL, 0);
	      continue;
	    }
	  ses = clib->clib_req_strses;
	  session_buffered_write (ses, (char *) i, len);
	}
    }
  else
    GPF_T1 ("dc type not supported from ks partition fast");
}

void
clrg_prepare_clibs (cl_req_group_t * clrg, data_source_t * qn, dtp_t * slices_done, slice_id_t * slices, int *rem, int first,
    int last, cl_op_t * clo, data_col_t * part_dc, int anify, dc_val_cast_t * cf)
{
  int set, init_rows;
  caddr_t *save;
  cll_in_box_t *clib;
  cl_op_t *param_clo;
  cl_host_t *host;
  cluster_map_t *clm = clrg->clrg_clm;
  for (set = first; set < last; set++)
    {
      slice_id_t slid = slices[set];
      cl_slice_t *csl;
      if (BIT_IS_SET (slices_done, slid))
	{
	  cll_in_box_t *clib = CLRG_CSL_CLIB (clrg, slid, 0);
	  clib->clib_part_n_rows++;
	  continue;
	}
      BIT_SET (slices_done, slid);
      csl = CLM_ID_TO_CSL (clm, slid);
      host = local_cll.cll_local;
      init_rows = (local_cll.cll_this_host == host->ch_id && !clo->_.frag.nth_stage) ? 200 : 0;
      param_clo = clrg_csl_clo (clrg, qn, host, csl, clo, NULL, init_rows, cf);
      clib = CLRG_CSL_CLIB (clrg, slid, clm->clm_host_rank[host->ch_id]);
      clib->clib_part_n_rows = 1;
      if (!init_rows)
	{
	  int n_params = BOX_ELEMENTS (param_clo->_.frag.params);
	  save = param_clo->_.frag.params;
	  param_clo->_.frag.params = NULL;
	  cl_serialize_cl_op_t (clib->clib_req_strses, param_clo);
	  param_clo->_.frag.params = save;
	  clib->clib_n_selects = 1;
	  clib->clib_n_selects_received = 0;
	  dks_array_head (clib->clib_req_strses, n_params, DV_ARRAY_OF_POINTER);
	  clib_start_dc (clib, part_dc, anify);
	}
      else
	clib->clib_is_local = 1;
    }
}

long tc_vec_part;
long tc_vec_part_fast;

int
ks_vec_partition_fast (key_source_t * ks, itc_cluster_t * itcl, data_source_t * qn, cl_op_t * clo, int n_rows, dc_val_cast_t * cf)
{
  /* no nulls, 1st is a single partition key dc */
  float cost;
  dtp_t slices_done[CL_MAX_SLICES / 8];
  cl_req_group_t *clrg = itcl->itcl_clrg;
  caddr_t *inst = itcl->itcl_qst;
  uint32 hash;
  key_partition_def_t *kpd = ks->ks_key->key_partition;
  col_partition_t *cp = kpd->kpd_cols[0];
  state_slot_t **params;
  int n_params, min_rows;
  int set_nos[ARTM_VEC_LEN];
  int rem[ARTM_VEC_LEN];
  data_col_t *part_dc = QST_BOX (data_col_t *, inst, ks->ks_vec_source[0]->ssl_index);
  int set, inx, nth;
  slice_id_t *slices;
  cluster_map_t *clm = ks->ks_key->key_partition->kpd_map;
  QN_CHECK_SETS (qn, inst, n_rows);
  slices = QST_BOX (slice_id_t *, inst, qn->src_sets);	/* can be set above by qn check sets */
  memset (&slices_done, 0, ALIGN_8 (kpd->kpd_map->clm_n_slices) / 8);
  if (SSL_REF == ks->ks_vec_source[0]->ssl_type)
    {
      for (set = 0; set < n_rows; set += ARTM_VEC_LEN)
	{
	  int last = MIN (set + ARTM_VEC_LEN, n_rows);
	  sslr_n_consec_ref (inst, ks->ks_vec_source[0], set_nos, set, last - set);
	  if (DV_LONG_INT == part_dc->dc_dtp || DV_IRI_ID == part_dc->dc_dtp)
	    {
	      if (CP_WORD == cp->cp_type)
		{
		  for (inx = 0; inx < last - set; inx++)
		    {
		      int64 i = ((int64 *) part_dc->dc_values)[set_nos[inx]];
		      hash = cp_int_any_hash (cp, i, &rem[inx]);
		      slices[set + inx] = clm->clm_slices[hash % clm->clm_n_slices]->csl_id;
		    }
		}
	      else
		{
		  for (inx = 0; inx < last - set; inx++)
		    {
		      int64 i = ((int64 *) part_dc->dc_values)[set_nos[inx]];
		      hash = cp_int_hash (cp, i, &rem[inx]);
		      slices[set + inx] = clm->clm_slices[hash % clm->clm_n_slices]->csl_id;
		    }
		}
	    }
	  else if (DV_ANY == part_dc->dc_dtp)
	    {
	      for (inx = 0; inx < last - set; inx++)
		{
		  hash = cp_any_hash (cp, ((db_buf_t *) part_dc->dc_values)[set_nos[inx]], &rem[inx]);
		  slices[set + inx] = clm->clm_slices[hash % clm->clm_n_slices]->csl_id;
		}
	    }
	  else
	    return 0;
	  clrg_prepare_clibs (clrg, qn, slices_done, slices, rem, set, last, clo, part_dc, NULL != cf[0], cf);
	  clrg_partition_dc (clrg, part_dc, set_nos, slices, set, last, 0, NULL != cf[0]);
	}
    }
  else
    {
      for (set = 0; set < n_rows; set += ARTM_VEC_LEN)
	{
	  int last = MIN (set + ARTM_VEC_LEN, n_rows);
	  if (DV_LONG_INT == part_dc->dc_dtp || DV_IRI_ID == part_dc->dc_dtp)
	    {
	      if (CP_WORD == cp->cp_type)
		{
		  for (inx = 0; inx < last - set; inx++)
		    {
		      int64 i = ((int64 *) part_dc->dc_values)[inx + set];
		      hash = cp_int_any_hash (cp, i, &rem[inx]);
		      slices[set + inx] = clm->clm_slices[hash % clm->clm_n_slices]->csl_id;
		    }
		}
	      else
		{
		  for (inx = 0; inx < last - set; inx++)
		    {
		      int64 i = ((int64 *) part_dc->dc_values)[inx + set];
		      hash = cp_int_hash (cp, i, &rem[inx]);
		      slices[set + inx] = clm->clm_slices[hash % clm->clm_n_slices]->csl_id;
		    }
		}
	    }
	  else if (DV_ANY == part_dc->dc_dtp)
	    {
	      for (inx = 0; inx < last - set; inx++)
		{
		  hash = cp_any_hash (cp, ((db_buf_t *) part_dc->dc_values)[inx + set], &rem[inx]);
		  slices[set + inx] = clm->clm_slices[hash % clm->clm_n_slices]->csl_id;
		}
	    }
	  else
	    return 0;
	  clrg_prepare_clibs (clrg, qn, slices_done, slices, rem, set, last, clo, part_dc, NULL != cf[0], cf);
	  clrg_partition_dc (clrg, part_dc, NULL, slices, set, last, 0, NULL != cf[0]);
	}
    }
  clrg_dc_done (clrg);
  for (nth = 1; nth < BOX_ELEMENTS (ks->ks_vec_source); nth++)
    {
      state_slot_ref_t *ssl = ks->ks_vec_source[nth];
      data_col_t *dc = QST_BOX (data_col_t *, inst, ks->ks_vec_source[nth]->ssl_index);
      clrg_start_dc (clrg, dc, NULL != cf[nth]);
      if (SSL_REF == ssl->ssl_type)
	{
	  for (set = 0; set < n_rows; set += ARTM_VEC_LEN)
	    {
	      int last = MIN (n_rows, set + ARTM_VEC_LEN);
	      sslr_n_consec_ref (inst, ssl, set_nos, set, last - set);
	      clrg_partition_dc (clrg, dc, set_nos, slices, set, last, nth, NULL != cf[nth]);
	    }
	}
      else if (SSL_VEC == ssl->ssl_type)
	{
	  clrg_partition_dc (clrg, dc, NULL, slices, 0, n_rows, nth, NULL != cf[nth]);
	}
      else
	GPF_T1 ("nust be ref or vec for partition fast");
      clrg_dc_done (clrg);
    }
  if (IS_QN (qn, query_frag_input))
    {
      QNCAST (query_frag_t, qf, qn);
      cost = qf->qf_cost;
      params = qf->qf_params;
    }
  else if (IS_QN (qn, stage_node_input))
    {
      QNCAST (stage_node_t, stn, qn);
      cost = stn->stn_cost;
      params = stn->stn_params;
    }
  min_rows = qp_thread_min_usec / (1 + (cost * compiler_unit_msecs * (float) 1000));
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
  {
    if (!clib->clib_waiting)
      continue;
    if (clib->clib_part_n_rows > min_rows)
      clib->clib_vec_clo->_.frag.merits_thread = 1;
  }
  END_DO_SET ();
  n_params = BOX_ELEMENTS (params);
  for (nth = nth; nth < n_params; nth++)
    {
      state_slot_t *ssl = params[nth];
      caddr_t scalar = qst_get (clrg->clrg_itcl->itcl_qst, ssl);
      DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
      {
	if (!clib->clib_waiting)
	  continue;
	if (clib->clib_dc_start)
	  print_object (scalar, clib->clib_req_strses, NULL, NULL);
      }
      END_DO_SET ();
    }
  if (ks->ks_last_vec_param && ks->ks_last_vec_param->ssl_sets)
    ssl_consec_results (ks->ks_last_vec_param, inst, n_rows);
  TC (tc_vec_part_fast);
  return 1;
}


int
ks_is_ssl_in_specs (key_source_t * ks, state_slot_t * ssl)
{
  /* true if ssl in search specs.  Means that if so, a typed dc must in fact be anified, else can pass as typed */
  search_spec_t *sp;
  for (sp = ks->ks_spec.ksp_spec_array; sp; sp = sp->sp_next)
    if (ssl == sp->sp_min_ssl || ssl == sp->sp_max_ssl)
      return 1;
  for (sp = ks->ks_row_spec; sp; sp = sp->sp_next)
    if (ssl == sp->sp_min_ssl || ssl == sp->sp_max_ssl)
      return 1;
  return 0;
}

void
ks_vec_partition (key_source_t * ks, itc_cluster_t * itcl, data_source_t * qn, cl_op_t * clo)
{
  /* cast and align the search param columns, partition cols are first, compute partition and add the data to the clib for the partition.  */
  cl_op_t *prev_target_clo = NULL;
  int32 rem = 0;
  caddr_t *inst = itcl->itcl_qst;
  QNCAST (query_instance_t, qi, inst);
  cl_op_t *prev_row_clo = NULL;
  cl_req_group_t *clrg = itcl->itcl_clrg;
  key_partition_def_t *kpd = ks->ks_key->key_partition;
  cl_op_t *target_clo = NULL;
  cluster_map_t *clm = kpd->kpd_map;
  caddr_t err = NULL;
  int row;
  int n_rows = qn->src_prev ? QST_INT (inst, qn->src_prev->src_out_fill) : qi->qi_n_sets;
  int n_cast = BOX_ELEMENTS (ks->ks_vec_cast), inx, n_cols = 0;
  data_col_t *target[MAX_PARAMS];
  data_col_t *source[MAX_PARAMS];
  dc_val_cast_t cf[MAX_PARAMS];
  state_slot_ref_t *sslr[MAX_PARAMS];
  dtp_t dc_elt_len[MAX_PARAMS];
  int cast_or_null = 0;
  int n_part_cols = BOX_ELEMENTS (ks->ks_key->key_partition->kpd_cols);
  int org_part_cols = n_part_cols;
  clrg_target_clm (clrg, kpd->kpd_map);
  if (!n_rows && !qn->src_prev)
    n_rows = 1;
  if (ks->ks_last_vec_param)
    QST_INT (inst, ks->ks_last_vec_param->ssl_n_values) = 0;
  for (inx = 0; inx < n_cast; inx++)
    {
      state_slot_ref_t *source_ref = ks->ks_vec_source[inx];
      data_col_t *source_dc = source[n_cols] = QST_BOX (data_col_t *, inst, ((state_slot_t *) source_ref)->ssl_index);
      data_col_t *target_dc = target[n_cols] = QST_BOX (data_col_t *, inst, ks->ks_vec_cast[inx]->ssl_index);
      sslr[n_cols] = SSL_REF == ks->ks_vec_source[inx]->ssl_type ? ks->ks_vec_source[inx] : NULL;
      dc_elt_len[n_cols] = dc_elt_size (source_dc);
      dc_reset (target_dc);
      cf[n_cols] = ks->ks_dc_val_cast[inx];
      if (target_dc->dc_dtp == source_dc->dc_dtp)
	cf[n_cols] = NULL;
      if (cf[n_cols] || source_dc->dc_any_null)
	cast_or_null = 1;
      if (!cf[n_cols] && DV_ANY == target_dc->dc_dtp && DV_ANY != source_dc->dc_dtp)
	{
	  if (ks_is_ssl_in_specs (ks, ks->ks_vec_cast[inx]))
	    cf[n_cols] = vc_to_any (source_dc->dc_dtp);
	  else
	    cf[n_cols] = vc_generic;
	}
      n_cols++;
      if (n_cols >= MAX_PARAMS)
	sqlr_new_error ("37000", "VEC..", "Too many searchh parameters");
    }
  qi->qi_n_sets = n_rows;
  qf_clrg_reuse (qn, ks, inst, clrg, cf);
  if (enable_ksp_fast && !cast_or_null && !ks->ks_is_flood && !ks->ks_scalar_partition && !clo->_.frag.is_update
      && 1 == clm->clm_n_replicas && 1 == org_part_cols)
    {
      if (ks_vec_partition_fast (ks, itcl, qn, clo, n_rows, cf))
	return;
    }
  TC (tc_vec_part);
  for (row = 0; row < n_rows; row++)
    {
      uint32 hash = 0;
      for (inx = 0; inx < n_cols; inx++)
	{
	  data_col_t *target_dc = target[inx];
	  data_col_t *source_dc = source[inx];
	  int source_row = row;
	  state_slot_ref_t *ref;
	  DC_CHECK_LEN (target_dc, target_dc->dc_n_values);
	  if ((ref = sslr[inx]))
	    {
	      int step;
	      for (step = 0; step < ref->sslr_distance; step++)
		{
		  int *set_nos = (int *) inst[ref->sslr_set_nos[step]];
		  source_row = set_nos[source_row];
		}
	    }
	  if (!cf[inx])
	    {
	      if (source_dc->dc_any_null)
		{
		  if (DV_ANY == source_dc->dc_dtp)
		    {
		      if (DV_DB_NULL == ((db_buf_t *) source_dc->dc_values)[source_row][0])
			goto was_null;
		      ((db_buf_t *) target_dc->dc_values)[target_dc->dc_n_values++] =
			  ((db_buf_t *) source_dc->dc_values)[source_row];
		    }
		  else if (source_dc->dc_type & DCT_BOXES)
		    {
		      if (DV_DB_NULL == DV_TYPE_OF (((db_buf_t *) source_dc->dc_values)[source_row]))
			goto was_null;
		      ((caddr_t *) target_dc->dc_values)[target_dc->dc_n_values++] =
			  box_mt_copy_tree (((db_buf_t *) source_dc->dc_values)[source_row]);
		    }
		  else
		    {
		      if (DC_IS_NULL (source_dc, source_row))
			goto was_null;
		      DC_ELT_MT_CPY (target_dc, target_dc->dc_n_values, source_dc, source_row, dc_elt_len[inx]);
		      target_dc->dc_n_values++;
		    }
		}
	      else
		{
		  DC_ELT_MT_CPY (target_dc, target_dc->dc_n_values, source_dc, source_row, dc_elt_len[inx]);
		  target_dc->dc_n_values++;
		}
	    }
	  else
	    {
	      if (!cf[inx] (target_dc, source_dc, source_row, &err))
		{
		  int inx2;
		  if (err && !qi->qi_no_cast_error)
		    sqlr_resignal (err);
		  dk_free_tree (err);
		  err = NULL;
		was_null:
		  {
		    char null_action = ks->ks_cast_null ? ks->ks_cast_null[inx] : 0;
		    if (null_action)
		      {
			dc_append_null (target[inx]);
			goto next_col;
		      }
		  cancel_row:
		    for (inx2 = 0; inx2 < inx; inx2++)
		      {
			/* check if it is DCT_BOXES & free the thing */
			if (DCT_BOXES & target[inx2]->dc_type)
			  dk_free_tree (((caddr_t *) target[inx2]->dc_values)[target[inx2]->dc_n_values - 1]);
			target[inx2]->dc_n_values--;
		      }
		    goto next_row;	/* the row of input is skipped */
		  }
		}
	    }
	next_col:
	  if (!ks->ks_is_flood && inx < n_part_cols)
	    {
	      int32 rem = 0;
	      hash = hash ^ dc_part_hash (ks->ks_vec_cp[inx], target[inx], target[inx]->dc_n_values - 1, &rem);
	      if (inx == n_part_cols - 1)
		{
		  int hi;
		  cl_slice_t *csl = clm->clm_slices[hash % clm->clm_n_slices];
		  cl_host_t *host = local_cll.cll_local;
		  target_clo = clrg_csl_clo (clrg, qn, host, csl, clo, NULL, n_rows, cf);
		  if (!prev_row_clo || prev_row_clo->_.frag.params != target_clo->_.frag.params)
		    {
		      clrg_target_dcs (clrg, target_clo, n_part_cols, ks->ks_vec_cast, target);
		      prev_row_clo = target_clo;
		    }
		}
	    }
	}
      if (ks->ks_last_vec_param && ks->ks_last_vec_param->ssl_sets)
	ssl_result (ks->ks_last_vec_param, inst, row);
    next_row:;
    }
  qi->qi_set_mask = NULL;
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
  {
    if (!clib->clib_vec_clo || 0 == clo_frag_n_sets (clib->clib_vec_clo))
      {
	clib->clib_waiting = 0;
	t_set_delete (&clo->clo_clibs, (void *) clib);
      }
  }
  END_DO_SET ();
}



void
cli_set_slice (client_connection_t * cli, cluster_map_t * clm, slice_id_t slice, caddr_t * err_ret)
{
  if (err_ret)
    *err_ret = NULL;
  cli->cli_slice = slice;
}
