/*
 *  cldfg.c
 *
 *  $Id$
 *
 *  Cluster non-colocated query frag
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
#include "sqlo.h"
#include "rdfinf.h"
#include "aqueue.h"

long tc_dfg_max_empty_mores;
long tc_dfg_empty_mores;
long tc_dfg_more_while_running;


#define in_printf(q)
#if 1
int enable_dfg_print = 0;
int enable_rec_dfg_print = 0;
#define dfg_printf(q) {if (enable_dfg_print) printf q; }
#else
#define dfg_printf(q)
#endif



extern long tc_dfg_coord_pause;
extern long tc_dfg_more;


int
stn_n_input (stage_node_t * stn, caddr_t * inst)
{
  rbuf_t *rbuf = QST_BOX (rbuf_t *, inst, stn->stn_rbuf->ssl_index);
  return rbuf ? rbuf->rb_count : 0;
}





stage_node_t *
qf_nth_stage (stage_node_t ** nodes, int nth)
{
  if (!nodes || BOX_ELEMENTS (nodes) < nth || nth < 1)
    GPF_T1 ("looking for s dfg stage that does not exist");
  return nodes[nth - 1];
}


void
stn_in_batch (stage_node_t * stn, caddr_t * inst, cl_message_t * cm, caddr_t in, int read_to, int bytes)
{
  /* add a batch string to the in queue of the stn */
  cl_op_t *clo = clo_allocate (CLO_STN_IN);
  cl_op_t *dfg_state;
  rbuf_t *rbuf = QST_BOX (rbuf_t *, inst, stn->stn_rbuf->ssl_index);
  int fill;
  clo->_.stn_in.in = in;
  clo->_.stn_in.read_to = read_to;
  clo->_.stn_in.bytes = bytes;
  if (cm)
    {
      clo->_.stn_in.in = cm->cm_in_string;
      clo->_.stn_in.bytes = cm->cm_bytes;
      clo->_.stn_in.in_list = cm->cm_in_list;
      clo->_.stn_in.n_comp = cm->cm_n_comp_entries;
    }
  if (!rbuf)
    {
      QST_BOX (rbuf_t *, inst, stn->stn_rbuf->ssl_index) = rbuf = rbuf_allocate ();
      rbuf->rb_free_func = (rbuf_free_t) dk_free_box;
    }
  rbuf_add (rbuf, (void *) clo);
  SRC_IN_STATE ((data_source_t *) stn, inst) = inst;
  if (rbuf->rb_count > 10)
    ((QI *) inst)->qi_slice_merits_thread = 1;
}



void
stn_reset_if_enough (stage_node_t * stn, caddr_t * inst, cl_req_group_t * clrg)
{
  /*return; */
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
  {
    int n;
    if ((n = clib->clib_n_selects) > ((stn->clb.clb_batch_size / 4) + 1) * 5)
      {
	QNCAST (query_instance_t, qi, inst);
	dfg_printf (("host %d slice %d stage %d resets after sending %d to %d\n", local_cll.cll_this_host, ((QI *) inst)->qi_slice,
		stn->stn_nth, n, clib->clib_host->ch_id));
	longjmp_splice (qi->qi_thread->thr_reset_ctx, RST_ENOUGH);
      }
  }
  END_DO_SET ();
}


#define dfg_slice_set_thread(inst, cm) \
  if (CMR_DFG_PARALLEL & cm->cm_req_flags) ((QI*)inst)->qi_slice_merits_thread = 1;



void
stn_divide_bulk_input (stage_node_t * stn, caddr_t * inst)
{
  QNCAST (QI, qi, inst);
  int coord;
  rbuf_t *bulk;
  if (qi->qi_slice_needs_init)
    {
      qi->qi_slice_needs_init = 0;
      qi_vec_init (qi, 1);
    }
  bulk = (rbuf_t *) QST_GET_V (inst, stn->stn_bulk_input);
  if (!bulk)
    return;

  coord = QST_INT (inst, stn->stn_coordinator_id);
  if (!coord)
    GPF_T1 ("dfg slice must specify coordinator");
  DO_RBUF (cl_message_t *, cm, rbe, rbe_inx, bulk)
  {
    stage_node_t *target_stn;
    if (local_cll.cll_this_host == coord)
      target_stn = qf_nth_stage (stn->stn_qf->qf_stages, cm->cm_dfg_stage);
    else
      target_stn = qf_nth_stage (stn->src_gen.src_query->qr_stages, cm->cm_dfg_stage);
    cm_record_dfg_deliv (cm, CM_DFGD_SELF);
    dfg_slice_set_thread (inst, cm);
    stn_in_batch (target_stn, inst, cm, NULL, 0, 0);
    dk_free_box (cm->cm_cl_stack);
    dk_free ((caddr_t) cm, sizeof (cl_message_t));
  }
  END_DO_RBUF;
  rbuf_delete_all (bulk);
}

void
stn_new_in_batch (stage_node_t * stn, caddr_t * inst, cl_op_t * clo)
{
  /* set the dres to the dcs, assign the scalars */
  int inx, nth_dre = 0, n_rows = 1;
  dc_read_t *dre_array = QST_BOX (dc_read_t *, inst, stn->stn_dre->ssl_index);
  if (!stn->stn_in_slots)
    GPF_T1 ("a stn must have in slots, at least the set no");
  if (!dre_array)
    {
      dre_array = (dc_read_t *) dk_alloc_box_zero (sizeof (dc_read_t) * dk_set_length (stn->stn_in_slots), DV_BIN);
      QST_BOX (dc_read_t *, inst, stn->stn_dre->ssl_index) = dre_array;
    }
  DO_BOX (state_slot_t *, param, inx, stn->stn_inner_params)
  {
    if (SSL_VEC == param->ssl_type)
      {
	caddr_t box = clo->_.frag.params[inx];
	db_buf_t str = *(db_buf_t *) box;
	dc_read_t *dre = &dre_array[nth_dre++];
	int null_flag_bytes = 0;
	memzero (dre, sizeof (dc_read_t));
	dre->dre_bytes = LONG_REF_NA (str);
	n_rows = dre->dre_n_values = LONG_REF_NA (str + 4);
	dre->dre_dtp = str[8];
	dre->dre_type = str[9];
	dre->dre_non_null = str[10];
	dre->dre_any_null = str[11];
	if (dre->dre_any_null && dre->dre_dtp != DV_ANY && !(DCT_BOXES & dre->dre_type))
	  {
	    dre->dre_nulls = str + 12;
	    null_flag_bytes = (ALIGN_8 (dre->dre_n_values) / 8);
	  }
	dre->dre_data = str + 12 + null_flag_bytes;
      }
    else
      {
	qst_set (inst, param, clo->_.frag.params[inx]);
	clo->_.frag.params[inx] = NULL;
      }
  }
  END_DO_BOX;
  dk_free_tree ((caddr_t) clo->_.frag.params);
  memzero (clo, sizeof (cl_op_t));
  clo->clo_op = CLO_ROW;
  clo->_.row.n_rows = n_rows;
}


int dfg_batch_from_queue (stage_node_t * stn, caddr_t * inst, int64 final_dfg_id);

#define dfg_inc_completed(msg)

void
stn_check_dcs (stage_node_t * stn, caddr_t * inst)
{
  int n = BOX_ELEMENTS (stn->stn_inner_params);
  int expect = QST_BOX (data_col_t *, inst, stn->stn_inner_params[0]->ssl_index)->dc_n_values, inx;
  for (inx = 1; inx < n; inx++)
    {
      data_col_t *dc;
      state_slot_t *ssl = stn->stn_inner_params[inx];
      if (SSL_VEC != ssl->ssl_type)
	continue;
      dc = QST_BOX (data_col_t *, inst, ssl->ssl_index);
      if (dc->dc_n_values != expect)
	GPF_T1 ("uneven dcs in stn reading");
    }
}


void
stn_in_uncompress (cl_op_t * in)
{
}


void
stn_roj_outer (stage_node_t * stn, caddr_t * inst)
{
  hash_source_t *hs = qn_next_qn (stn, hash_source_input);
  SRC_IN_STATE (stn, inst) = NULL;
  QST_INT (inst, hs->hs_roj_state) = 1;
  QST_INT (inst, hs->clb.clb_nth_set) = 0;
  QST_INT (inst, hs->hs_roj) = 0;
  hash_source_roj_input (hs, inst, NULL);
}


void
stn_extend_batch (stage_node_t * stn, caddr_t * inst, itc_cluster_t * itcl, dc_read_t * dre)
{
  /* the output batch must be the whole input clo worth of values, the gby read node following dependds on this */
  int batch = dre->dre_n_values, inx;
  itcl->itcl_batch_size = batch;
  QST_INT (inst, stn->src_gen.src_batch_size) = batch;
  DO_BOX (state_slot_t *, ssl, inx, stn->stn_inner_params)
  {
    if (SSL_VEC == ssl->ssl_type)
      {
	data_col_t *dc = QST_BOX (data_col_t *, inst, ssl->ssl_index);
	DC_CHECK_LEN (dc, batch);
      }
  }
  END_DO_BOX;
}


int dfg_clo_mp_limit = 10000000;
void
stn_continue (stage_node_t * stn, caddr_t * inst)
{
  /* read up to full batch from the inputs where left off, send to successor, increment the count of processed for each full input consumed.  A batch is fully consumed only when there is nothing left and stn continue is called again for more, proving that subsequent steps of the pipeline have no continuable state, so it is not consumed yet when it is sent onwards   */
  QNCAST (QI, qi, inst);
  dc_read_t *dre;
  cl_op_t *dfg_state;
  cll_in_box_t clib;
  cl_ses_t clses;
  dk_session_t *ses = &clib.clib_in_strses;
  int ctr;
  int no_dc_break = stn->stn_loc_ts->ts_order_ks->ks_is_flood && IS_QN (qn_next (stn), chash_read_input);
  cl_op_t *itcl_clo = (cl_op_t *) qst_get (inst, stn->clb.clb_itcl);
  itc_cluster_t *itcl = itcl_clo ? itcl_clo->_.itcl.itcl : NULL;
  stn_divide_bulk_input (stn, inst);
  if (!itcl)
    {
      itcl_clo = clo_allocate (CLO_ITCL);
      itcl = itcl_clo->_.itcl.itcl = itcl_allocate (qi->qi_trx, inst);
      itcl->itcl_pool->mp_max_bytes = dfg_clo_mp_limit;
      qst_set (inst, stn->clb.clb_itcl, (caddr_t) itcl_clo);
      ITCL_TRACE (itcl);
    }
  else
    ITCL_TRACE (itcl);
  memzero (&clses, sizeof (clses));
  memzero (&clib, sizeof (clib));
  clib.clib_vectored = 1;
  clib.clib_itcl = itcl;
  clib.clib_dc_read = QST_BOX (dc_read_t *, inst, stn->stn_dre->ssl_index);
  clses.clses_reading_req_clo = 1;
  SESSION_SCH_DATA (&clib.clib_in_strses) = &clib.clib_in_siod;
  DKS_CL_DATA (&clib.clib_in_strses) = &clses;
  DKS_QI_DATA (&clib.clib_in_strses) = (query_instance_t *) inst;
  dfg_inc_completed ("at continue");
  if (stn->stn_roj_outer && QST_INT (inst, stn->stn_roj_outer))
    {
      stn_roj_outer (stn, inst);
      return;
    }
new_batch:
  itcl->itcl_n_results = 0;
  dc_reset_array (inst, (data_source_t *) stn, stn->stn_inner_params, -1);
  itcl->itcl_batch_size = QST_INT (inst, stn->src_gen.src_batch_size);
again:
  for (;;)
    {
      cl_op_t *in = (cl_op_t *) qst_get (inst, stn->stn_current_input);
      ITCL_TRACE (itcl);
      if (!in)
	{
	  rbuf_t *rbuf = QST_BOX (rbuf_t *, inst, stn->stn_rbuf->ssl_index);
	  if (rbuf && rbuf->rb_count)
	    {
	      in = rbuf_get (rbuf);
	      qst_set (inst, stn->stn_current_input, (caddr_t) in);
	      stn_in_uncompress (in);
	    }
	  else
	    {
	      QST_INT (inst, stn->stn_state) = STN_INIT;
	      if (!itcl->itcl_n_results)
		SRC_IN_STATE ((data_source_t *) stn, inst) = NULL;
	      ITCL_TRACE (itcl);
	      if (dfg_batch_from_queue (stn, inst, 0))
		{
		  int dummy = 0;
		  /* the queue may have given input to later nodes.  These must continue before this one returns */
		  stn_divide_bulk_input (stn, inst);
		  if (!itcl->itcl_n_results)
		    {
		      dfg_resume_pending (stn->src_gen.src_query, (query_instance_t *) inst,
			  stn->stn_qf ? stn->stn_qf->qf_nodes : stn->src_gen.src_query->qr_nodes, &dummy, stn);
		      dfg_printf (("host %d got queued input slice %d stage %d, resume successors = %d\n", local_cll.cll_this_host,
			      qi->qi_slice, stn->stn_nth, dummy));
		      ITCL_TRACE (itcl);
		    }
		  if (stn_n_input (stn, inst))
		    dfg_printf (("host %d slice %d  stage %d queued input for self\n", local_cll.cll_this_host, qi->qi_slice,
			    stn->stn_nth));
		  continue;	/* go check if this stn got input from queue */
		}
	      ITCL_TRACE (itcl);
	      if (itcl->itcl_n_results)
		goto full_batch;	/* can be a partial batch read before running out of input */
	      return;
	    }
	}

      ses->dks_in_buffer = in->_.stn_in.in;
      ses->dks_in_read = in->_.stn_in.read_to;
      ses->dks_in_fill = in->_.stn_in.bytes;
      CATCH_READ_FAIL (ses)
      {
	while (ses->dks_in_read < in->_.stn_in.bytes)
	  {
	    if ((dre = (dc_read_t *) QST_GET_V (inst, stn->stn_dre)) && dre->dre_pos < dre->dre_n_values)
	      {
		stn_check_dcs (stn, inst);
	      more_from_dre:
		clib_vec_read_into_slots (&clib, inst, stn->stn_in_slots);
		stn_check_dcs (stn, inst);
		ITCL_TRACE (itcl);
		if (dre->dre_pos >= dre->dre_n_values)
		  {
		    dfg_printf (("slice %d stage %d consumed %d rows continuing\n", qi->qi_slice, stn->stn_nth, dre->dre_n_values));
		    qst_set (inst, stn->stn_current_input, NULL);
		    CL_ONLY (dfg_state->_.dfg_stat.batches_consumed++);
		    QST_INT (inst, stn->stn_n_ins_in_out)++;
		  }
		else if (no_dc_break)
		  {
		    /* a dfg can begin with a partitioned gby reader.  If so, the first stage is a broadcast from coord and if the gby is multistate this has a vector size equal to the number of sets in the gby.
		     * The stn in this case must produce a single batch of with one row per set in the gby, so adjust the batch size to be more than this */
		    stn_extend_batch (stn, inst, itcl, dre);
		    goto more_from_dre;
		  }
		if (itcl->itcl_batch_size == itcl->itcl_n_results)
		  goto full_batch;
		goto again;
	      }
	    else if (CLO_QF_EXEC == ses->dks_in_buffer[ses->dks_in_read])
	      {
		SAVE_READ_FAIL (ses);
		clib_read_next (&clib, NULL, NULL);
		RESTORE_READ_FAIL (ses);
		stn_new_in_batch (stn, inst, &clib.clib_first);
		clib.clib_dc_read = QST_BOX (dc_read_t *, inst, stn->stn_dre->ssl_index);
		stn_check_dcs (stn, inst);
	      more_from_dre2:
		clib_vec_read_into_slots (&clib, inst, stn->stn_in_slots);
		stn_check_dcs (stn, inst);
		dre = QST_BOX (dc_read_t *, inst, stn->stn_dre->ssl_index);
		if (dre->dre_pos >= dre->dre_n_values)
		  {
		    dfg_printf (("slice %d stage %d consumed %d rows\n", qi->qi_slice, stn->stn_nth, dre->dre_n_values));
		    CL_ONLY (dfg_state->_.dfg_stat.batches_consumed++);
		    QST_INT (inst, stn->stn_n_ins_in_out)++;
		    qst_set (inst, stn->stn_current_input, NULL);
		  }
		else if (no_dc_break)
		  {
		    /* a dfg can begin with a partitioned gby reader.  If so, the first stage is a broadcast from coord and if the gby is multistate this has a vector size equal to the number of sets in the gby.
		     * The stn in this case must produce a single batch of with one row per set in the gby, so adjust the batch size to be more than this */
		    stn_extend_batch (stn, inst, itcl, dre);
		    goto more_from_dre2;
		  }
		if (itcl->itcl_n_results >= itcl->itcl_batch_size)
		  goto full_batch;
		goto again;
	      }
	    else if (CLO_QF_PREPARE == ses->dks_in_buffer[ses->dks_in_read])
	      {
		/* in principle, might get the prepare from the coordinator but even before this a 2nd req from non coord so as to fetch the qf by other means so here ignore this */
		cl_op_t *prep = cl_deserialize_cl_op_t (ses);
		qr_free (prep->_.qf_prepare.qr);
		dk_free_box ((caddr_t) prep);
		in->_.stn_in.read_to = ses->dks_in_read;
		continue;
	      }
	    else
	      GPF_T1 (" a stn batch is made of opt qf prepare, qf execs and arrays for next params and nothing else");
	  }
      }
      FAILED
      {
	sqlr_error ("42000", "stn input string read failed, probably cluster message corrupted in transmission");
      }
      END_READ_FAIL (ses);

      qst_set (inst, stn->stn_current_input, NULL);
      in = NULL;
    }
full_batch:
  QST_INT (inst, stn->src_gen.src_out_fill) = 0;
  if (!SRC_IN_STATE (stn, inst))
    dfg_inc_completed ("at sending final set");
  for (ctr = 0; ctr < itcl->itcl_n_results; ctr++)
    qn_result ((data_source_t *) stn, inst, -1);	/* -1 means a dc ref past this will gpf, there can be no ssl refs across a partitioning node */
  qn_send_output ((data_source_t *) stn, inst);
  dfg_inc_completed ("at return of successor");
  itcl->itcl_n_results = 0;
  goto new_batch;
}


cl_aq_ctx_t *
claq_copy (cl_aq_ctx_t * claq)
{
  NEW_VAR (cl_aq_ctx_t, cpy);
  *cpy = *claq;
  cpy->claq_rec_thread = NULL;
  cpy->claq_of_parent = 0;
  cpy->claq_is_allocated = 1;
  return cpy;
}


void
claq_free (cl_aq_ctx_t * claq)
{
  if (claq->claq_is_allocated)
    dk_free ((caddr_t) claq, sizeof (cl_aq_ctx_t));
}

void
aq_dfg_cleanup (caddr_t av)
{
  caddr_t *args = (caddr_t *) av;
  caddr_t *inst = (caddr_t *) unbox (args[0]);
  QNCAST (query_instance_t, qi, inst);
  cl_aq_ctx_t *claq = (cl_aq_ctx_t *) unbox (args[1]);
  qi->qi_thread = NULL;
  claq->claq_of_parent = 2;	/* set as if this had run even though not started due to anytime or other interruption setting the aq to no more, else will fail assert later */
}

caddr_t
aq_dfg_local_func (caddr_t av, caddr_t * err_ret)
{
  /* the qi to continue, the qf, the lt w id, the qi on the main thread */
  caddr_t *args = (caddr_t *) av;
  caddr_t *inst = (caddr_t *) unbox (args[0]);
  QNCAST (query_instance_t, qi, inst);
  cl_aq_ctx_t *claq = (cl_aq_ctx_t *) unbox (args[1]);
  query_frag_t *qf = (query_frag_t *) claq->claq_qr;
  cluster_map_t *clm = qn_loc_ts (qf->qf_head_node, 1)->ts_order_ks->ks_key->key_partition->kpd_map;
  client_connection_t *cli = cl_cli ();
  async_queue_t *aq = cli->cli_aqr->aqr_aq;
  if (qi->qi_dfg_running)
    GPF_T1 ("double run of dfg slice");
  qi->qi_dfg_running = 1;
  qi->qi_trx = cli->cli_trx;
  if (claq->claq_main_trx_no != qi->qi_trx->lt_trx_no)
    {
      /* a local dfg can run on the thread of the coord branch.  If so leave the lt as is */
      qi->qi_trx->lt_rc_w_id = claq->claq_rc_w_id;
      qi->qi_trx->lt_main_trx_no = claq->claq_main_trx_no;
      qi->qi_trx->lt_cl_main_enlisted = claq->claq_enlist;
    }
  cli->cli_claq = claq;
  dk_free_tree (av);
  qi->qi_client = cli;
  if (cli->cli_anytime_started)
    cli->cli_anytime_qf_started = get_msec_real_time ();
  IN_CLL;
  qi->qi_thread = THREAD_CURRENT_THREAD;
  qi->qi_threads = 1;
  LEAVE_CLL;
  QR_RESET_CTX
  {
    int any = 0;
    cli_set_slice (cli, clm, qi->qi_slice, NULL);
    if (!aq->aq_no_more)
      dfg_resume_pending (qf->src_gen.src_query, (query_instance_t *) inst, qf->qf_nodes, &any, NULL);
  }
  QR_RESET_CODE
  {
    if (RST_ERROR == reset_code)
      {
	qi->qi_proc_ret = thr_get_error_code (qi->qi_thread);
	aq->aq_no_more = 1;
      }
  }
  END_QR_RESET;
  claq->claq_of_parent = 2;
  cli->cli_claq = NULL;
  claq_free (claq);
  IN_CLL;
  qi->qi_dfg_running = 0;
  qi->qi_thread = NULL;
  qi->qi_threads = 0;
  LEAVE_CLL;
  cli_set_slice (cli, NULL, QI_NO_SLICE, NULL);
  return (caddr_t) NULL;
}




void
qi_qf_vec_init (query_instance_t * qi, query_frag_t * qf, int n_sets)
{
  qi_vec_init (qi, n_sets);
}

query_t *
qr_top_qr (query_t * qr)
{
  while (qr->qr_super)
    qr = qr->qr_super;
  return qr;
}


caddr_t *
qf_slice_qi (query_frag_t * qf, caddr_t * inst, int is_in_cll)
{
  QNCAST (query_instance_t, qi, inst);
  caddr_t *cp = (caddr_t *) dk_alloc_box_zero (qi->qi_query->qr_instance_length, DV_QI);
  QNCAST (query_instance_t, cpqi, cp);
  query_t *qr = qi->qi_query;
  if (!is_in_cll)
    IN_CLL;
  qr->qr_ref_count++;
  if (!is_in_cll)
    LEAVE_CLL;
  cpqi->qi_is_allocated = 1;
  cpqi->qi_is_branch = 1;
  cpqi->qi_is_dfg_slice = 1;
  cpqi->qi_root_id = qi->qi_root_id;
  cpqi->qi_query = qr;		/* always the top qr since taken from the a running qi */
  cpqi->qi_ref_count = 1;
  if (is_in_cll)
    cpqi->qi_slice_needs_init = 1;
  else
    qi_vec_init (cpqi, 1);
  DO_SET (state_slot_t *, ssl, &qr->qr_temp_spaces)
  {
    switch (ssl->ssl_type)
      {
      case SSL_TREE:
	qst_set (cp, ssl, box_copy_tree (qst_get (inst, ssl)));
	break;
      }
  }
  END_DO_SET ();
  cpqi->qi_no_cast_error = qi->qi_no_cast_error;
  cpqi->qi_isolation = qi->qi_isolation;
  cpqi->qi_lock_mode = qi->qi_lock_mode;
  cpqi->qi_u_id = qi->qi_u_id;
  cpqi->qi_g_id = qi->qi_g_id;
  qst_set_hash_part (cp, inst, qi->qi_query);
  return cp;
}

int
clo_frag_is_empty (cl_op_t * clo)
{
  int inx;
  if (CLO_QF_EXEC != clo->clo_op)
    return 0;
  DO_BOX (data_col_t *, dc, inx, clo->_.frag.params) if (DV_DATA == DV_TYPE_OF (dc))
    return 0 == dc->dc_n_values;
  END_DO_BOX;
  return 0;
}


int
clib_frag_is_empty (cll_in_box_t * clib)
{
  if (clib->clib_part_n_rows)
    return 0;
  return clo_frag_is_empty (clib->clib_vec_clo);
}

cl_message_t *
clib_dfg_cm (cll_in_box_t * clib, int is_reply, int is_first_stn, stage_node_t * stn)
{
  slice_id_t slice;
  int64 bytes;
  caddr_t in_str;
  cl_req_group_t *clrg = clib->clib_group;
  cl_message_t *cm;
  if (clib_frag_is_empty (clib))
    return NULL;
  if (clib->clib_dc_start)
    ;
  else if (local_cll.cll_this_host == clib->clib_host->ch_id)
    cl_serialize_cl_op_t (clib->clib_req_strses, clib->clib_vec_clo);	/* going to local, no qf sending and must have serialization, no memory copy */
  else
    clo_serialize (clib, clib->clib_vec_clo);
  bytes = strses_length (clib->clib_req_strses);
  /*NEW_VARZ (cl_message_t, cm); */
  cm = dk_alloc (sizeof (cl_message_t));
  memset (cm, 0, sizeof (cl_message_t));
  cm->cm_bytes = bytes;
  slice = clib->clib_slice;
  cm->cm_slice = slice;
  cm->cm_dfg_coord = clib->clib_vec_clo->_.frag.coord_host;
  in_str = cl_msg_string (cm->cm_bytes + 1);
  strses_to_array (clib->clib_req_strses, in_str);
  strses_flush (clib->clib_req_strses);
  if (clib->clib_vec_clo->_.frag.merits_thread)
    cm->cm_req_flags = CMR_DFG_PARALLEL;
  cm->cm_in_string = in_str;
  if (is_reply)
    cm->cm_op = CL_RESULT;
  else
    {
      cm->cm_op = CL_BATCH;
      if (is_first_stn)
	cm->cm_cl_stack = (cl_call_stack_t *) box_copy ((caddr_t) clib->clib_group->clrg_lt->lt_client->cli_cl_stack);
      else
	{
	  cl_call_stack_t *clst = clib->clib_group->clrg_lt->lt_client->cli_cl_stack;
	  int len = box_length (clst) - sizeof (cl_call_stack_t);
	  cm->cm_cl_stack = dk_alloc_box (len, DV_BIN);
	  memcpy (cm->cm_cl_stack, clst, len);
	}
    }
  CM_SET_QUOTA (clib->clib_group, cm, clib->clib_group->clrg_send_time);
  cm->cm_req_no = clib->clib_group->clrg_dfg_req_no;
  cm->cm_from_host = local_cll.cll_this_host;
  cm->cm_is_error = 0;
  cm->cm_enlist |= clrg->clrg_cm_control;
  clib->clib_is_active = 1;
  cm->cm_req_flags |= CMR_DFG;
  cm_record_send (cm, local_cll.cll_this_host);
  return cm;
}


QI **
qi_slice_extend (caddr_t * inst, state_slot_t * slice_qis, slice_id_t slice, QI ** old_qis)
{
  QI **qis = (QI **) dk_alloc_box_zero ((10 + slice) * sizeof (caddr_t), DV_ARRAY_OF_POINTER);
  QST_BOX (QI **, inst, slice_qis->ssl_index) = qis;
  if (old_qis)
    {
      memcpy_16 (qis, old_qis, box_length (old_qis));
      dk_free_box ((caddr_t) old_qis);
    }
  return qis;
}


caddr_t *
stn_find_slice (state_slot_t * slice_qis, caddr_t * inst, slice_id_t slice)
{
  query_instance_t **qis = (query_instance_t **) QST_GET_V (inst, slice_qis);
  if (!qis || BOX_ELEMENTS (qis) <= slice)
    qis = qi_slice_extend (inst, slice_qis, slice, qis);
  return (caddr_t *) qis[slice];
}


caddr_t *
stn_add_slice_inst (state_slot_t * slice_qis, query_frag_t * qf, caddr_t * inst, int coordinator, slice_id_t slice, int is_in_cll)
{
  QI *cpqi;
  caddr_t *cp;
  QI **qis = QST_BOX (QI **, inst, slice_qis->ssl_index);
  if (is_in_cll)
    ASSERT_IN_CLL;
  if (local_cll.cll_this_host == coordinator)
    cp = qf_slice_qi (qf, inst, is_in_cll);
  else
    cp = qf_slice_qi (NULL, inst, is_in_cll);
  cpqi = (QI *) cp;
  cpqi->qi_slice = slice;
  if (!qis || BOX_ELEMENTS (qis) <= slice)
    qis = qi_slice_extend (inst, slice_qis, slice, qis);
  qis[slice] = cpqi;
  return cp;
}

async_queue_t *
stn_dfg_aq (stage_node_t * stn, caddr_t * inst)
{
  QNCAST (QI, qi, inst);
  async_queue_t *aq = QST_BOX (async_queue_t *, inst, stn->stn_aq->ssl_index);
  if (!aq)
    {
      aq = aq_allocate (qi->qi_client, qi_n_cl_aq_threads (qi));
      aq->aq_do_self_if_would_wait = 1;
      lt_timestamp (qi->qi_trx, aq->aq_lt_timestamp);
      aq->aq_ts = get_msec_real_time ();
      if (stn->stn_qf)
	{
	  if (qi->qi_isolation >= ISO_REPEATABLE || stn->stn_qf->qf_need_enlist || PL_EXCLUSIVE == stn->stn_qf->qf_lock_mode)
	    aq->aq_max_threads = 0;
	}
      else if (!enable_mt_txn && (qi->qi_isolation >= ISO_REPEATABLE || PL_EXCLUSIVE == stn->src_gen.src_query->qr_lock_mode))
	aq->aq_max_threads = 0;
      qst_set (inst, stn->stn_aq, (caddr_t) aq);
    }
  return aq;
}


caddr_t *
stn_slice_inst (stage_node_t * stn, caddr_t * inst, slice_id_t slice, int in_cll)
{
  caddr_t *slice_inst = stn_find_slice (stn->stn_slice_qis, inst, slice);
  if (slice_inst)
    return slice_inst;
  return stn_add_slice_inst (stn->stn_slice_qis, stn->stn_qf, inst, QST_INT (inst, stn->stn_coordinator_id), slice, in_cll);
}


void
stn_start_slice (stage_node_t * stn, cl_aq_ctx_t * claq, caddr_t * slice_inst)
{
  caddr_t *main_inst = claq->claq_main_inst;
  async_queue_t *aq = stn_dfg_aq (stn, main_inst);
  QNCAST (QI, main_qi, main_inst);
  if (((QI *) slice_inst)->qi_dfg_running)
    GPF_T1 ("restart running dfg slice");

  if (!main_qi->qi_is_dfg_slice && !((QI *) slice_inst)->qi_slice_merits_thread)
    aq->aq_queue_only = 1;
  else
    main_qi->qi_client->cli_activity.da_qp_thread++;
  /* check not entirely correct, the window after the return of a dfg slice can be a false positive */
  /*aq_check_duplicate (aq, (caddr_t)slice_inst); */
  claq = claq_copy (claq);
  if (local_cll.cll_this_host == QST_INT (main_inst, stn->stn_coordinator_id))
    aq_request (aq, aq_dfg_local_func, list (2, box_num ((ptrlong) slice_inst), box_num ((ptrlong) claq)));
}


int
dfg_other_should_start (stage_node_t * stn, caddr_t * inst, caddr_t * slice_inst, cl_message_t * cm, int coord)
{
  /* a slice can get input but if it is at full batch and the last stage is ot continuable nor is the message for the last stage then do not start it, just deliver the cm to it */
  stage_node_t **stages;
  if (!((QI *) inst)->qi_is_dfg_slice)
    return 1;
  stages = local_cll.cll_this_host == coord ? stn->stn_qf->qf_stages : stn->src_gen.src_query->qr_stages;
  if (BOX_ELEMENTS (stages) == cm->cm_dfg_stage)
    return 1;
  if (QST_INT (slice_inst, stn->stn_out_bytes) < cl_dfg_batch_bytes)
    return 1;
  return 0;
}


void
dfg_ssm_not_known (stage_node_t * stn, caddr_t * inst, cl_message_t * cm)
{
}


#define TA_DFG_FREE 1234

typedef struct dfg_free_s
{
  void *dfgf_cms;
  void *dfgf_boxes;
} dfg_free_t;


#define dfgf_cm_free(dfgf, cm) { *(void**)cm = dfgf->dfgf_cms; dfgf->dfgf_cms = (void*)cm;}
#define dfgf_free_box(dfgf, box) { if (IS_BOX_POINTER (box)) { *(void**)box = dfgf->dfgf_boxes; dfgf->dfgf_boxes = (void*)box;}}

int dfg_recd[100];
int enable_itc_dfg_ck;
long tc_dfg_start_other;
long tc_itc_start_other;
long tc_dfg_create_other;
long tc_dfg_feed_self;
long tc_feed_no_action;
long tc_dfg_skip_by_bit;
long tc_run_rb_alloc;
long tc_slice_rb_alloc;
long tc_rbuf_append;
long tc_dfg_feed;
long tc_dfg_start_len;
long tc_dfg_exit_len;
long tc_dfg_feed_1;


#if 1
#define DFG_TC(ctr) ctr++
#define DFG_FEED_TRACE dfg_recd[(unsigned short)cm->cm_from_host % (sizeof (dfg_recd) / sizeof (int))]++;
#define DFG_ADD(ct, s) ct += s
#define DFG_DETAIL
#else
#define DFG_TC(ctr)
#define DFG_FEED_TRACE
#define DFG_ADD(ct, s)
#endif


/* pre-sorted input for dfgs with key req_no, coord, slice as 64 bit word, dep is dk_set_t of dv_bin with fill and n queued cms */
dk_hash_t *dfg_running_queue;
int enable_dfg_follows = 1;
#define enable_dfg_queue_running 1

#ifdef RUN_QUEUE_REUSE

void
stn_append_bulk (stage_node_t * stn, caddr_t * inst, rbuf_t * queue)
{
  caddr_t *place = &inst[stn->stn_bulk_input->ssl_index];
  rbuf_t *prev = (rbuf_t *) * place;
  if (!prev)
    prev = *place = (caddr_t) resource_get (cll_rbuf_rc);
  if (prev->rb_count)
    GPF_T1 ("expected empty rbuf from cll rbuf rc");
  DFG_TC (tc_rbuf_append);
  rbuf_append (prev, queue);
}

int
dfg_get_queued (caddr_t * inst, stage_node_t * stn, int coord, uint32 req_no, int free_only)
{
  QNCAST (QI, qi, inst);
  int64 key = req_no | (((int64) coord) << 32) | (((int64) qi->qi_slice) << 48);
  ptrlong pqueue;
  rbuf_t *queue;
  if (!coord)
    GPF_T1 ("get queued without coord");
  ASSERT_IN_CLL;
  GETHASH ((void *) key, dfg_running_queue, pqueue, not_found);
  queue = (rbuf_t *) pqueue;
  if (free_only)
    {
      remhash ((void *) key, dfg_running_queue);
#ifdef MTX_METER
      DO_RBUF (cl_message_t *, cm, rbe, rbe_inx, queue)
      {
	cm_record_dfg_deliv (cm, CM_DFGD_DISCARD);
      }
      END_DO_RBUF;
#endif
      rbuf_delete_all (queue);
      resource_store (cll_rbuf_rc, (void *) queue);
      LEAVE_CLL;
    }
  else
    {
      if (!queue->rb_count)
	return 0;
      stn_append_bulk (stn, inst, queue);
    }
  return 1;
not_found:
  if (free_only)
    LEAVE_CLL;
  return 0;
}
#else

void
stn_append_bulk (stage_node_t * stn, caddr_t * inst, rbuf_t * queue)
{
  caddr_t *place = &inst[stn->stn_bulk_input->ssl_index];
  if (!*place)
    *place = (caddr_t) queue;
  else
    {
      rbuf_t *prev = (rbuf_t *) * place;
      DFG_TC (tc_rbuf_append);
      rbuf_append (prev, queue);
      resource_store (cll_rbuf_rc, (void *) queue);
    }
}

int
dfg_get_queued (caddr_t * inst, stage_node_t * stn, int coord, uint32 req_no, int free_only)
{
  QNCAST (QI, qi, inst);
  int64 key = req_no | (((int64) coord) << 32) | (((int64) qi->qi_slice) << 48);
  ptrlong pqueue;
  if (!coord)
    GPF_T1 ("get queued without coord");
  ASSERT_IN_CLL;
  GETHASH ((void *) key, dfg_running_queue, pqueue, not_found);

  remhash ((void *) key, dfg_running_queue);
  if (free_only)
    {
#ifdef MTX_METER
      DO_RBUF (cl_message_t *, cm, rbe, rbe_inx, ((rbuf_t *) pqueue))
      {
	cm_record_dfg_deliv (cm, CM_DFGD_DISCARD);
      }
      END_DO_RBUF;
#endif
      rbuf_delete_all ((rbuf_t *) pqueue);
      resource_store (cll_rbuf_rc, (caddr_t) pqueue);
      LEAVE_CLL;
    }
  else
    stn_append_bulk (stn, inst, (rbuf_t *) pqueue);
  return 1;
not_found:
  if (free_only)
    LEAVE_CLL;
  return 0;
}

#endif


rbuf_t *
cm_rbuf_allocate ()
{
  rbuf_t *rb;
  ASSERT_IN_CLL;
  rb = (rbuf_t *) resource_get (cll_rbuf_rc);
  if (rb->rb_count)
    GPF_T1 ("expected to get empty rbuf from cll rbuf rc");
  rb->rb_free_func = (rbuf_free_t) cm_free;
  return rb;
}

rbuf_t *
dfg_queue_for_running (int coord, uint32 req_no, cl_message_t * cm)
{
  int64 key = req_no | (((int64) coord) << 32) | (((int64) cm->cm_slice) << 48);
  ptrlong pqueue;
  rbuf_t *queue;
  ASSERT_IN_CLL;
  GETHASH ((void *) key, dfg_running_queue, pqueue, not_found);
  cm_record_dfg_deliv (cm, CM_DFGD_R_QUEUE);
  queue = (rbuf_t *) pqueue;
  rbuf_add ((rbuf_t *) pqueue, (void *) cm);
  return (rbuf_t *) pqueue;
not_found:
  queue = cm_rbuf_allocate ();
  DFG_TC (tc_run_rb_alloc);
  sethash ((void *) key, dfg_running_queue, (void *) queue);
  rbuf_add (queue, (void *) cm);
  return queue;
}


void
qi_free_dfg_queue_nodes (query_instance_t * qi, dk_set_t nodes)
{
  /* in qi free, for a dfg branch killed before its time, empty any it may have received while in running state */
  DO_SET (stage_node_t *, stn, &nodes)
  {
    if (IS_QN (stn, stage_node_input) && 1 == stn->stn_nth)
      {
	uint32 req_no = unbox_inline (QST_GET_V (qi, stn->stn_coordinator_req_no));
	int coord = QST_INT (qi, stn->stn_coordinator_id);
	rbuf_t *queue = QST_BOX (rbuf_t *, qi, stn->stn_bulk_input->ssl_index);
	if (!req_no)
	  continue;
	if (!coord)
	  GPF_T1 ("dfg branch being free must specify a coord");
	IN_CLL;
	if (queue && !queue->rb_count)
	  {
	    /* empty queue is put for reuse, a non-empty will be freed outsid of cll */
	    resource_store (cll_rbuf_rc, (void *) queue);
	    QST_BOX (rbuf_t *, qi, stn->stn_bulk_input->ssl_index) = NULL;
	  }
	/* in del only mode, below leave cll */
	dfg_get_queued ((caddr_t *) qi, stn, coord, req_no, 1);
      }
    else if (IS_QN (stn, query_frag_input))
      qi_free_dfg_queue_nodes (qi, ((query_frag_t *) stn)->qf_nodes);
    else if (IS_QN (stn, subq_node_input))
      qi_free_dfg_queue_nodes (qi, ((subq_source_t *) stn)->sqs_query->qr_nodes);
  }
  END_DO_SET ();
}


void
qi_free_dfg_queue (query_instance_t * qi, query_t * qr)
{
  qi_free_dfg_queue_nodes (qi, qr->qr_nodes);
  DO_SET (query_t *, subq, &qr->qr_subq_queries) qi_free_dfg_queue (qi, subq);
  END_DO_SET ();
}


void
dfg_after_feed ()
{
}

int enable_feed_other_dfg;

int
dfg_feed_other_dfg (cl_message_t * cm, dk_hash_t ** other_dfg_ret)
{
  rbuf_t *queue;
  int64 k;
  dk_hash_t *other_dfg;
  if (!enable_feed_other_dfg || !cm->cm_dfg_stage || QI_NO_SLICE == cm->cm_slice || !cm->cm_dfg_coord)
    return 0;
  if (!(other_dfg = *other_dfg_ret))
    other_dfg = *other_dfg_ret = hash_table_allocate (101);
  k = (((int64) cm->cm_slice) << 32) + cm->cm_req_no;
  queue = (rbuf_t *) gethash ((void *) k, other_dfg);
  if (!queue)
    {
      sethash ((void *) k, other_dfg, (void *) 1);
      return 0;
    }
  if (1 == (int64) queue)
    {
      rbuf_t *queu = dfg_queue_for_running (cm->cm_dfg_coord, cm->cm_req_no, cm);
      sethash ((void *) k, other_dfg, (void *) queue);
      return 1;
    }
  else
    {
      rbuf_add (queue, cm);
      return 1;
    }
}


rbuf_t *
qi_slice_queue (caddr_t * slice_inst, stage_node_t * stn)
{
  rbuf_t *rb = QST_BOX (rbuf_t *, slice_inst, stn->stn_bulk_input->ssl_index);
  if (!rb)
    {
      rb = QST_BOX (rbuf_t *, slice_inst, stn->stn_bulk_input->ssl_index) = cm_rbuf_allocate ();
      DFG_TC (tc_slice_rb_alloc);
    }
  return rb;
}


#define CLQ_DEL_INLINE \
  {new_count--; rbe->rbe_data[rbe_inx] = NULL; if (rbe_inx == new_rbe_read) new_rbe_read = RBE_NEXT (rbe, new_rbe_read);}


#define INIT_QUEUES \
  { memzero (slice_queues, sizeof (caddr_t) * max_slice); queues_inited = 1;}

#define SLICE_QUEUES(queue) \
{  \
	      if (!queues_inited && ctr > 4) \
		INIT_QUEUES; \
	      if (queues_inited) \
		slice_queues[slice] = queue; \
}


#define RET_CK if (!ctr) goto ret;



long tc_dfg_feed_short_clocks;

int
dfg_feed_short (stage_node_t * stn, caddr_t * inst, cl_queue_t * bsk,
    caddr_t * main_inst, uint32 dfg_req_no, rbuf_t ** slice_queues, int *any_for_other_ret, int max_slice, int coord)
{
  dk_hash_t *other_dfg = NULL;
#ifdef DFG_DETAIL
  int64 start_ts = rdtsc ();
#endif
  du_thread_t *self = THREAD_CURRENT_THREAD;
  QNCAST (QI, qi, inst);
  rbuf_t *queue = NULL;
  caddr_t *slice_inst = NULL;
  int queues_inited = 0;
  int any_for_self = 0, any_for_other = 0;
  rbuf_t *main_bulk = NULL;
  int ctr = bsk->rb_count;
  int new_count = ctr;
  rbuf_elt_t *rbe = bsk->rb_first;
  int new_read = rbe->rbe_read;
  QI **slice_qis = QST_BOX (QI **, main_inst, stn->stn_slice_qis->ssl_index);
  int max_slice_qi = slice_qis ? BOX_ELEMENTS (slice_qis) : 0;
  int new_rbe_read = rbe->rbe_read, rbe_inx;
  for (rbe_inx = rbe->rbe_read; ctr; rbe_inx = RBE_NEXT (rbe, rbe_inx))
    {
      cl_message_t *cm = rbe->rbe_data[rbe_inx];
      if (!cm)
	continue;
      ctr--;

      if (dfg_req_no == cm->cm_req_no)
	{
	  slice_id_t slice = cm->cm_slice;
	  if (cm->cm_dfg_agg_req_no)
	    {
	      /* a dfg agg read message is sent with the dfg req no and flags to get properly serialized with a post-end hanging cl more.  Do not confuse with a feed message */
	      cl_agg_trace (cm->cm_dfg_agg_req_no);
	      continue;
	    }
	  if (QI_NO_SLICE == slice && CLO_DFG_ARRAY == cm->cm_in_string[0] && qi->qi_is_dfg_slice)
	    {
	      if (!main_bulk)
		main_bulk = qi_slice_queue (main_inst, stn);
	      rbuf_add (main_bulk, (void *) cm);
	      CLQ_DEL_INLINE;
	      if (main_bulk->rb_count > 1000)
		QST_INT (inst, stn->stn_out_bytes) = cl_dfg_batch_bytes + 10;
	      continue;
	    }
	  if (slice > max_slice)
	    {
	      MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 4);
	      continue;
	    }
#if 0
	  if (cm->cm_to_host && cm->cm_to_host != local_cll.cll_this_host)
	    log_error ("Host %d: Recd cm from %d meant for host %d", local_cll.cll_this_host, cm->cm_from_host, cm->cm_to_host);
#endif
#ifdef MTX_METER
	  if ((4L << 32) & cm->cm_queue_ts)
	    bing ();
#endif
	  if ((CL_MORE == cm->cm_op && cm->cm_from_host == coord) || (cm->cm_cancelled && cm->cm_dfg_coord == coord))
	    {
	      if (CL_MORE == cm->cm_op)
		TC (tc_dfg_more_while_running);
	      cm_record_dfg_deliv (cm, CM_DFGD_DISCARD);
	      cm_free (cm);
	      CLQ_DEL_INLINE;
	      continue;
	    }
	  if (CL_BATCH == cm->cm_op && coord != cm->cm_dfg_coord)
	    {
	      MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 3);
	      continue;
	    }
	  if (!cm->cm_dfg_stage)
	    {
	      MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 5);
	      goto next;
	    }
	  if (queues_inited && slice_queues[slice])
	    {
	      cm_record_dfg_deliv (cm, CM_DFGD_OTHER);
	      rbuf_add (slice_queues[slice], (void *) cm);
	      CLQ_DEL_INLINE;
	      continue;
	    }
	  slice_inst = (caddr_t *) (slice < max_slice_qi ? slice_qis[slice] : NULL);
#if 0
	  if (!slice_inst && !CLM_ID_TO_CSL (stn_clm, cm->cm_slice))
	    log_error ("Host %d: dfg cm frm %d to slice %d, not here", local_cll.cll_this_host, cm->cm_from_host, cm->cm_slice);
#endif
	  if (slice_inst && ((QI *) slice_inst)->qi_threads && slice_inst != inst)
	    {
	      rbuf_t *queue = dfg_queue_for_running (coord, dfg_req_no, cm);
	      CLQ_DEL_INLINE;
	      RET_CK;
	      SLICE_QUEUES (queue);
	      continue;
	    }
	  if (local_cll.cll_this_host == coord && !qi->qi_is_dfg_slice)
	    dfg_ssm_not_known (stn, inst, cm);
	  if (!qi->qi_is_dfg_slice)
	    {
	      if (!slice_inst)
		slice_inst = stn_add_slice_inst (stn->stn_slice_qis, stn->stn_qf, inst, coord, slice, 1);
	      QST_INT (slice_inst, stn->stn_coordinator_id) = coord;
	      qst_set_long (slice_inst, stn->stn_coordinator_req_no, unbox (QST_GET_V (inst, stn->stn_coordinator_req_no)));
	      dfg_slice_set_thread (slice_inst, cm);
	      dfg_printf (("stn_in: coord feeds slice %d stage %d req %d:%d from %d\n", slice, stn->stn_nth, cm->cm_dfg_coord,
		      cm->cm_req_no, cm->cm_from_host));
	      DFG_FEED_TRACE;
	      cm_record_dfg_deliv (cm, CM_DFGD_START);
	      queue = qi_slice_queue (slice_inst, stn);
	      rbuf_add (queue, (void *) cm);
	      CLQ_DEL_INLINE;
	      RET_CK;
	      SLICE_QUEUES (queue);
	      slice_qis = QST_BOX (QI **, main_inst, stn->stn_slice_qis->ssl_index);
	      max_slice_qi = BOX_ELEMENTS (slice_qis);
	    }
	  else if (slice == qi->qi_slice)
	    {
	      dfg_slice_set_thread (inst, cm);
	      DFG_TC (tc_dfg_feed_self);
	      dfg_printf (("stn_in:  slice %d feeds self stage %d req %d:%d from %d\n", slice, stn->stn_nth, cm->cm_dfg_coord,
		      cm->cm_req_no, cm->cm_from_host));
	      DFG_FEED_TRACE;
	      cm_record_dfg_deliv (cm, CM_DFGD_SELF);
	      queue = qi_slice_queue (inst, stn);
	      rbuf_add (queue, (void *) cm);
	      CLQ_DEL_INLINE;
	      any_for_self = 1;
	      RET_CK;
	      SLICE_QUEUES (queue);
	    }
	  else
	    {
	      if (!slice_inst)
		{
		  slice_inst = stn_add_slice_inst (stn->stn_slice_qis, stn->stn_qf, main_inst, coord, slice, 1);
		  TC (tc_dfg_create_other);
		  QST_INT (slice_inst, stn->stn_coordinator_id) = coord;
		  qst_set_long (slice_inst, stn->stn_coordinator_req_no, unbox (QST_GET_V (main_inst,
			      stn->stn_coordinator_req_no)));
		  dfg_slice_set_thread (slice_inst, cm);
		  dfg_printf (("stn_in: %d creates slice %d stage %d req %d:%d from %d\n", qi->qi_slice, slice, stn->stn_nth,
			  cm->cm_dfg_coord, cm->cm_req_no, cm->cm_from_host));
		  DFG_FEED_TRACE;
		  cm_record_dfg_deliv (cm, CM_DFGD_START);
		  queue = qi_slice_queue (slice_inst, stn);
		  rbuf_add (queue, (void *) cm);
		  any_for_other = 1;
		  ((QI *) slice_inst)->qi_thread = self;
		  CLQ_DEL_INLINE;
		  RET_CK;
		  SLICE_QUEUES (queue);
		  slice_qis = QST_BOX (QI **, main_inst, stn->stn_slice_qis->ssl_index);
		  max_slice_qi = BOX_ELEMENTS (slice_qis);
		}
	      else if (!((QI *) slice_inst)->qi_threads
		  && (!((QI *) slice_inst)->qi_thread || self == ((QI *) slice_inst)->qi_thread))
		{
		  if (dfg_other_should_start (stn, inst, slice_inst, cm, coord))
		    {
		      ((QI *) slice_inst)->qi_thread = self;
		      TC (tc_dfg_start_other);
		      dfg_printf (("stn_in: slice %d restarts slice %d stage %d req %d:%d from %d\n", qi->qi_slice, slice,
			      stn->stn_nth, cm->cm_dfg_coord, cm->cm_req_no, cm->cm_from_host));
		      any_for_other = 1;
		    }
		  dfg_slice_set_thread (slice_inst, cm);
		  DFG_FEED_TRACE;
		  cm_record_dfg_deliv (cm, CM_DFGD_OTHER);
		  queue = qi_slice_queue (slice_inst, stn);
		  rbuf_add (queue, (void *) cm);
		  CLQ_DEL_INLINE;
		  RET_CK;
		  SLICE_QUEUES (queue);
		}
	      else
		{
		  MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 1);
		  goto next;
		}
	    }
	}
      else
	{
	  if (dfg_feed_other_dfg (cm, &other_dfg))
	    {
	      CLQ_DEL_INLINE;
	    }
	  MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 2);
	next:
	  TC (tc_feed_no_action);
	}
    }
ret:
  bsk->rb_count = new_count;
  rbe->rbe_write = rbe_inx;
  rbe->rbe_count = new_count;
  rbe->rbe_read = new_read;
  *any_for_other_ret += any_for_other;
  DFG_ADD (tc_dfg_feed_short_clocks, rdtsc () - start_ts);
  if (other_dfg)
    hash_table_free (other_dfg);
  return any_for_self;
}



int
dfg_feed (stage_node_t * stn, caddr_t * inst, cl_queue_t * bsk)
{
  /* if this is a main qi, feed all the slice qis. If this is a slice qi, feed self and others that are not running.
   * Can make a slice qi if none previously.  The fed others are marked with qi thred set to the feeding thread.  At the end they are started on the dfg aq.  In order not to be started twice, the qi threads is set to 1 when starting so sucj do not get fed by others anymore */
  dk_hash_t *other_dfg = NULL;
  du_thread_t *self = THREAD_CURRENT_THREAD;
  QNCAST (QI, qi, inst);
  dtp_t excluded[CL_MAX_SLICES / 8];
  rbuf_t *slice_queues_auto[1000];
  rbuf_t **slice_queues = &slice_queues_auto[0];
  key_source_t *ks = stn->stn_loc_ts->ts_order_ks;
  cluster_map_t *stn_clm =
      KS_FLOOD_HASH_FILL == ks->ks_is_flood ? qi->qi_client->cli_csl->csl_clm : ks->ks_key->key_partition->kpd_map;
  slice_id_t max_slice = stn_clm->clm_distinct_slices;
  int any_for_self = 0, any_for_other = 0;
  int coord = QST_INT (inst, stn->stn_coordinator_id);
  uint32 dfg_req_no = unbox_inline (QST_GET_V (inst, stn->stn_coordinator_req_no));
  caddr_t *main_inst = qi->qi_is_dfg_slice ? qi->qi_client->cli_claq->claq_main_inst : inst;
  caddr_t *slice_inst;
  rbuf_t *main_bulk = NULL;
  DFG_TC (tc_dfg_feed);
  DFG_ADD (tc_dfg_start_len, clq_count (bsk));
  if (enable_dfg_queue_running && qi->qi_is_dfg_slice)
    any_for_self = dfg_get_queued (inst, stn, coord, dfg_req_no, 0);
  else if (enable_dfg_queue_running)
    {
      int inx;
      caddr_t **slice_qis = QST_BOX (caddr_t **, inst, stn->stn_slice_qis->ssl_index);
      DO_BOX (caddr_t *, slice_inst, inx, slice_qis) if (slice_inst)
	any_for_other += dfg_get_queued (slice_inst, stn, coord, dfg_req_no, 0);
      END_DO_BOX;
    }
  ASSERT_IN_CLL;
  if (!clq_count (bsk))
    return qi->qi_is_dfg_slice ? any_for_self : any_for_other;
  if (bsk->rb_first && !bsk->rb_first->rbe_next)
    {
      DFG_TC (tc_dfg_feed_1);
      any_for_self += dfg_feed_short (stn, inst, bsk, main_inst, dfg_req_no, slice_queues, &any_for_other, max_slice, coord);
      goto feed_done;
    }
  memzero (slice_queues, max_slice * sizeof (caddr_t));
  max_slice = ALIGN_8 (max_slice);
  memzero (excluded, max_slice / 8);
  DO_CLQ (cl_message_t *, cm, elt, rbe_inx, bsk)
  {
    int stage;
    if (dfg_req_no == cm->cm_req_no)
      {
	slice_id_t slice = cm->cm_slice;
	if (cm->cm_dfg_agg_req_no)
	  {
	    /* a dfg agg read message is sent with the dfg req no and flags to get properly serialized with a post-end hanging cl more.  Do not confuse with a feed message */
	    cl_agg_trace (cm->cm_dfg_agg_req_no);
	    clq_next (bsk, elt, rbe_inx);
	    continue;
	  }
	if (QI_NO_SLICE == slice && qi->qi_is_dfg_slice && CLO_DFG_ARRAY == cm->cm_in_string[0])
	  {
	    if (!main_bulk)
	      main_bulk = qi_slice_queue (main_inst, stn);
	    rbuf_add (main_bulk, (void *) cm);
	    clq_delete (bsk, elt, rbe_inx);
	    if (main_bulk->rb_count > 1000)
	      QST_INT (inst, stn->stn_out_bytes) = cl_dfg_batch_bytes + 10;
	    continue;
	  }
	if (slice > max_slice || BIT_IS_SET (excluded, slice))
	  {
	    MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 4);
	    TC (tc_dfg_skip_by_bit);
	    clq_next (bsk, elt, rbe_inx);
	    continue;
	  }
	if (cm->cm_to_host && cm->cm_to_host != local_cll.cll_this_host)
	  log_error ("Host %d: Recd cm from %d meant for host %d", local_cll.cll_this_host, cm->cm_from_host, cm->cm_to_host);
#ifdef MTX_METER
	if ((4L << 32) & cm->cm_queue_ts)
	  bing ();
#endif
	if ((CL_MORE == cm->cm_op && cm->cm_from_host == coord) || (cm->cm_cancelled && cm->cm_dfg_coord == coord))
	  {
	    if (CL_MORE == cm->cm_op)
	      TC (tc_dfg_more_while_running);
	    cm_record_dfg_deliv (cm, CM_DFGD_DISCARD);
	    cm_free (cm);
	    clq_delete (bsk, elt, rbe_inx);
	    continue;
	  }
	if (CL_BATCH == cm->cm_op && coord != cm->cm_dfg_coord)
	  {
	    MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 3);
	    clq_next (clq, elt, rbe_inx);
	    continue;
	  }
	stage = cm->cm_dfg_stage;
	if (!stage)
	  {
	    MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 5);
	    goto next;
	  }
	if (slice_queues[slice])
	  {
	    cm_record_dfg_deliv (cm, CM_DFGD_OTHER);
	    rbuf_add (slice_queues[slice], (void *) cm);
	    clq_delete (bsk, elt, rbe_inx);
	    continue;
	  }
	slice_inst = stn_find_slice (stn->stn_slice_qis, main_inst, slice);
#if 0
	if (!slice_inst && !CLM_ID_TO_CSL (stn_clm, cm->cm_slice))
	  log_error ("Host %d: dfg cm frm %d to slice %d, not here", local_cll.cll_this_host, cm->cm_from_host, cm->cm_slice);
#endif
	if (slice_inst && ((QI *) slice_inst)->qi_threads && slice_inst != inst)
	  {
	    slice_queues[slice] = dfg_queue_for_running (coord, dfg_req_no, cm);
	    clq_delete (bsk, elt, rbe_inx);
	    continue;
	  }
	if (local_cll.cll_this_host == coord && !qi->qi_is_dfg_slice)
	  dfg_ssm_not_known (stn, inst, cm);
	if (!qi->qi_is_dfg_slice)
	  {
	    if (!slice_inst)
	      slice_inst = stn_add_slice_inst (stn->stn_slice_qis, stn->stn_qf, inst, coord, slice, 1);
	    QST_INT (slice_inst, stn->stn_coordinator_id) = coord;
	    qst_set_long (slice_inst, stn->stn_coordinator_req_no, unbox (QST_GET_V (inst, stn->stn_coordinator_req_no)));
	    dfg_slice_set_thread (slice_inst, cm);
	    dfg_printf (("stn_in: coord feeds slice %d stage %d req %d:%d from %d\n", slice, stn->stn_nth, cm->cm_dfg_coord,
		    cm->cm_req_no, cm->cm_from_host));
	    DFG_FEED_TRACE;
	    cm_record_dfg_deliv (cm, CM_DFGD_START);
	    slice_queues[slice] = qi_slice_queue (slice_inst, stn);
	    rbuf_add (slice_queues[slice], (void *) cm);
	    clq_delete (bsk, elt, rbe_inx);
	  }
	else if (slice == qi->qi_slice)
	  {
	    dfg_slice_set_thread (inst, cm);
	    DFG_TC (tc_dfg_feed_self);
	    dfg_printf (("stn_in:  slice %d feeds self stage %d req %d:%d from %d\n", slice, stn->stn_nth, cm->cm_dfg_coord,
		    cm->cm_req_no, cm->cm_from_host));
	    DFG_FEED_TRACE;
	    cm_record_dfg_deliv (cm, CM_DFGD_SELF);
	    slice_queues[slice] = qi_slice_queue (inst, stn);
	    rbuf_add (slice_queues[slice], (void *) cm);
	    clq_delete (bsk, elt, rbe_inx);
	    any_for_self = 1;
	  }
	else
	  {
	    if (!slice_inst)
	      {
		slice_inst = stn_add_slice_inst (stn->stn_slice_qis, stn->stn_qf, main_inst, coord, slice, 1);
		TC (tc_dfg_create_other);
		QST_INT (slice_inst, stn->stn_coordinator_id) = coord;
		qst_set_long (slice_inst, stn->stn_coordinator_req_no, unbox (QST_GET_V (main_inst, stn->stn_coordinator_req_no)));
		dfg_slice_set_thread (slice_inst, cm);
		dfg_printf (("stn_in: %d creates slice %d stage %d req %d:%d from %d\n", qi->qi_slice, slice, stn->stn_nth,
			cm->cm_dfg_coord, cm->cm_req_no, cm->cm_from_host));
		DFG_FEED_TRACE;
		cm_record_dfg_deliv (cm, CM_DFGD_START);
		slice_queues[slice] = qi_slice_queue (slice_inst, stn);
		rbuf_add (slice_queues[slice], (void *) cm);
		any_for_other = 1;
		((QI *) slice_inst)->qi_thread = self;
		clq_delete (bsk, elt, rbe_inx);
	      }
	    else if (!((QI *) slice_inst)->qi_threads
		&& (!((QI *) slice_inst)->qi_thread || self == ((QI *) slice_inst)->qi_thread))
	      {
		if (dfg_other_should_start (stn, inst, slice_inst, cm, coord))
		  {
		    ((QI *) slice_inst)->qi_thread = self;
		    TC (tc_dfg_start_other);
		    dfg_printf (("stn_in: slice %d restarts slice %d stage %d req %d:%d from %d\n", qi->qi_slice, slice,
			    stn->stn_nth, cm->cm_dfg_coord, cm->cm_req_no, cm->cm_from_host));
		    any_for_other = 1;
		  }
		dfg_slice_set_thread (slice_inst, cm);
		DFG_FEED_TRACE;
		cm_record_dfg_deliv (cm, CM_DFGD_OTHER);
		slice_queues[slice] = qi_slice_queue (slice_inst, stn);
		rbuf_add (slice_queues[slice], (void *) cm);
		clq_delete (bsk, elt, rbe_inx);
	      }
	    else
	      {
		BIT_SET (excluded, cm->cm_slice);
		MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 1);
		goto next;
	      }
	  }
      }
    else
      {
	MTX_TS_SET_2 (cm->cm_dfg_skip_ts, local_cll.cll_mtx, 2);
	if (dfg_feed_other_dfg (cm, &other_dfg))
	  {
	    clq_delete (bsk, elt, rbe_inx);
	    continue;
	  }
      next:
	TC (tc_feed_no_action);
	clq_next (bsk, elt, rbe_inx);
      }
  }
  END_DO_CLQ;
feed_done:
  if (any_for_other && qi->qi_is_dfg_slice)
    {
      QI **slice_qis = (QI **) QST_GET_V (main_inst, stn->stn_slice_qis);
      int inx;
      DO_BOX (QI *, slice_qi, inx, slice_qis)
      {
	if (slice_qi && self == slice_qi->qi_thread && slice_qi->qi_slice != qi->qi_slice)
	  {
	    if (slice_qi->qi_threads)
	      continue;		/* may have been queued for exec earlier by this thread, now residing in the aq queue with thread set to this one */
	    slice_qi->qi_threads = 1;
	    stn_start_slice (stn, qi->qi_client->cli_claq, (caddr_t *) slice_qi);
	  }
      }
      END_DO_BOX;
    }
  if (other_dfg)
    hash_table_free (other_dfg);
  DFG_ADD (tc_dfg_exit_len, clq_count (bsk));
  if (!qi->qi_is_dfg_slice)
    return any_for_self + any_for_other;
  return any_for_self;
}


void
cli_rec_dfg_done (client_connection_t * cli, int coord, int req_no, int in_cll, char *file, int line)
{
}


void
ks_set_dfg_queue_f (key_source_t * ks, caddr_t * inst, it_cursor_t * itc)
{
}

long tc_rec_dfg_cancel;
int
dfg_batch_from_queue (stage_node_t * stn, caddr_t * inst, int64 final_dfg_id)
{
  /* if a stage node is at end, could be more batches for this or other stages in the queue.  For coord, this is the results queue of the qf's itcl.  For non-coord, this is the clt's queue.  If find, pop one off and put it into the suitable stage's input */
  /* the idea is to prefer processing batches for later stages to continuing earlier stages */
  QNCAST (QI, qi, inst);
  caddr_t *main_inst = qi->qi_is_dfg_slice ? qi->qi_client->cli_claq->claq_main_inst : inst;
  int rc = 0;
  {
    query_frag_t *qf = stn->stn_qf;
    itc_cluster_t *itcl = ((cl_op_t *) QST_GET_V (main_inst, qf->clb.clb_itcl))->_.itcl.itcl;
    IN_CLL;
    DO_SET (cll_in_box_t *, clib, &itcl->itcl_clrg->clrg_clibs)
    {
      if (clq_count (&clib->clib_in))
	{
	  rc = dfg_feed (stn, inst, &clib->clib_in);
	  break;
	}
    }
    END_DO_SET ();
    LEAVE_CLL;
  }
  dfg_after_feed ();
  return rc;
}


int enable_nb_clrg = 0;


int
clrg_dfg_send (cl_req_group_t * clrg, int coord_host, int64 * bytes_ret, int is_first_stn, stage_node_t * stn)
{
  int64 n_bytes = 0;
  int hi, max_ch = 0, n_sent = 0, rc;
  caddr_t map = NULL;
  dk_session_t *req_strses;
  QNCAST (QI, qi, clrg->clrg_inst);
  caddr_t *inst = clrg->clrg_inst;
  dk_mutex_t *reply_mtx = NULL;
  int is_first = 1;
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
  {
    cl_op_t *clo = clib->clib_vec_clo;
    if (!clo || !clib->clib_waiting)
      continue;
    clib->clib_dfg_any = 1;

    /* coordinator too itself, may be different slice:  Add the req to results queue */
    cl_message_t *cm = clib_dfg_cm (clib, 1, is_first_stn, stn);
    cll_in_box_t *rcv_clib;
    if (!cm)
      continue;
    cm->cm_dfg_stage = stn->stn_nth;
    n_bytes += cm->cm_bytes;
    IN_CLL;
    rcv_clib = (cll_in_box_t *) gethash ((void *) (ptrlong) cm->cm_req_no, local_cll.cll_id_to_clib);
    if (!rcv_clib)
      {
	LEAVE_CLL;
	sqlr_new_error ("CLVEC", "CL...", "Self is not expecting dfg replies in sending dfg");
      }
    clq_add (&rcv_clib->clib_in, (void *) cm);
    cm_record_dispatch (cm, -1, CM_D_TOP_QUEUE);
    LEAVE_CLL;
    n_sent++;



  }
  END_DO_SET ();
  clrg->clrg_send_buffered = 0;
  clrg->clrg_all_sent = 1;
  *bytes_ret = n_bytes;
  return n_sent;
}

extern int enable_vec_reuse;

int
stn_start (stage_node_t * stn, caddr_t * inst)
{
  /* take a batch of input for the stn, partitions, sends.  Local sends go direct to queue.  */
  QNCAST (query_instance_t, qi, inst);
  cl_op_t clo;
  caddr_t err;
  cl_op_t *dfg_state;
  int target_host, n_parts = 0, n_sent;
  int64 n_bytes = 0;
  cl_op_t *itcl_clo = (cl_op_t *) QST_GET_V (inst, stn->clb.clb_itcl);
  itc_cluster_t *itcl;
  if (STN_RUN == QST_INT (inst, stn->stn_state))
    GPF_T1 ("a stn should not get input when in running mode");
  if (!stn->stn_loc_ts)
    stn->stn_loc_ts = sqlg_loc_ts ((table_source_t *) qn_next ((data_source_t *) stn), NULL);
  if (!itcl_clo)
    {
      itcl_clo = clo_allocate (CLO_ITCL);
      itcl = itcl_clo->_.itcl.itcl = itcl_allocate (qi->qi_trx, inst);
      itcl->itcl_pool->mp_max_bytes = dfg_clo_mp_limit;
      qst_set (inst, stn->clb.clb_itcl, (caddr_t) itcl_clo);
      itcl->itcl_clrg->clrg_keep_local_clo = 0;
      itcl->itcl_clrg->clrg_is_dfg = 1;
      itcl->itcl_clrg->clrg_need_enlist =
	  stn->stn_need_enlist | stn->src_gen.src_query->qr_need_enlist | qi->qi_query->qr_need_enlist;
    }
  else
    {
      itcl = itcl_clo->_.itcl.itcl;
      clrg_set_lt (itcl->itcl_clrg, qi->qi_trx);
      DO_SET (cll_in_box_t *, clib, &itcl->itcl_clrg->clrg_clibs) clib->clib_waiting = 0;
      END_DO_SET ();
    }
  itcl->itcl_clrg->clrg_inst = inst;
  itcl->itcl_qst = inst;
  if (1 != stn->stn_nth)
    {
      /* this is not a slice inst but the main inst, so not continuable here. */
      SRC_IN_STATE ((data_source_t *) stn, inst) = inst;
    }
  QST_INT (inst, stn->stn_state) = STN_INIT;
  memset (&clo, 0, sizeof (clo));
  clo.clo_op = CLO_QF_EXEC;
  if (KS_FLOOD_HASH_FILL == stn->stn_loc_ts->ts_order_ks->ks_is_flood)
    clo._.frag.is_update = KS_FLOOD_HASH_FILL;
  clo._.frag.qf = (query_frag_t *) stn;
  clo._.frag.qst = inst;
  clo._.frag.lock_mode = qi->qi_lock_mode;
  clo._.frag.coord_host = QST_INT (inst, stn->stn_coordinator_id);
  clo._.frag.isolation = qi->qi_isolation;
  clo._.frag.is_autocommit = qi->qi_client->cli_row_autocommit;
  if (!stn->stn_qf)
    clo._.frag.qf_id = stn->src_gen.src_query->qr_qf_id;
  clo._.frag.nth_stage = stn->stn_nth;
  clo._.frag.coord_req_no = unbox (QST_GET_V (inst, stn->stn_coordinator_req_no));
  ks_vec_partition (stn->stn_loc_ts->ts_order_ks, itcl, (data_source_t *) stn, &clo);
  DO_SET (cll_in_box_t *, target, &clo.clo_clibs)
  {
    n_parts++;
  }
  END_DO_SET ();
  clo.clo_clibs = NULL;		/* clibs from mem pool, no freeing */
  if (!n_parts)
    return 0;
  itcl->itcl_clrg->clrg_dfg_req_no = unbox (QST_GET_V (inst, stn->stn_coordinator_req_no));
  itcl->itcl_clrg->clrg_dfg_host = QST_INT (inst, stn->stn_coordinator_id);
  err = NULL;
  IO_SECT (qi)
  {
    n_sent =
	clrg_dfg_send (itcl->itcl_clrg, QST_INT (inst, stn->stn_coordinator_id), &n_bytes, QST_INT (inst,
	    stn->stn_coordinator_id) == local_cll.cll_this_host && 1 == stn->stn_nth, stn);
  }
  END_IO_SECT (&err);
  if (err)
    sqlr_resignal (err);
  if (n_sent != n_parts)
    GPF_T1 ("send and partition counts mismatch");
  if (enable_vec_reuse)
    {
      caddr_t *save = SRC_IN_STATE (stn, inst);
      SRC_IN_STATE (stn, inst) = NULL;
      qn_vec_reuse ((data_source_t *) stn, inst);
      SRC_IN_STATE (stn, inst) = save;
    }
  /* the first node of the coordinator must not start processing on its own even if batch full.  The continue may run only inside the cl_qf_local_next function which will send the dfg counts and handle resets etc.  Not running in this context will lose them and screw the counts.
   * So if the whole batch is local, you would start here but then do not start here because the caller cl_start_search will start and that in turn will run all in the right ctx */
  QST_INT (inst, stn->stn_out_bytes) += n_bytes;
  if (QST_INT (inst, stn->stn_coordinator_id) == local_cll.cll_this_host && 1 == stn->stn_nth)
    return 1;
  stn_continue (stn, inst);
  if (QST_INT (inst, stn->stn_out_bytes) > cl_dfg_batch_bytes)
    {
      dfg_printf (("Reset slice %d stage %d due to enough output\n", qi->qi_slice, stn->stn_nth));
      longjmp_splice (qi->qi_thread->thr_reset_ctx, RST_ENOUGH);
    }
  if ((++QST_INT (inst, stn->stn_reset_ctr) % 100) == 0)
    {
      dfg_printf ((" periodic reset slice %d stage %d every 100\n", qi->qi_slice, stn->stn_nth));
      longjmp_splice (qi->qi_thread->thr_reset_ctx, RST_ENOUGH);
    }
  stn_reset_if_enough (stn, inst, itcl->itcl_clrg);
  return 1;
}


void
stage_node_input (stage_node_t * stn, caddr_t * inst, caddr_t * state)
{
  if (!state)
    stn_continue (stn, inst);
  else
    stn_start (stn, inst);
}


void
stage_node_free (stage_node_t * stn)
{
  clb_free (&stn->clb);
  dk_set_free (stn->stn_in_slots);
  dk_free_box ((caddr_t) stn->stn_params);
  dk_free_box ((caddr_t) stn->stn_inner_params);
}

void
dfg_resume_pending (query_t * subq, query_instance_t * qi, dk_set_t nodes, int *any_done, stage_node_t * successors_only)
{
  /* for a dfg, continue the items after the last stage as long as they can be continued.  Continue non-last stages only if the total sent by this dfg batch is under the limit.
     If successors_only, then only nodes later than this are continuable.  This is when getting input from others, must always continue the latest continuable */
  stage_node_t *stn;
  caddr_t *inst = (caddr_t *) qi;
  int32 sent = 0;
cont_innermost_loop:
  stn = NULL;
  DO_SET (data_source_t *, src, &nodes)
  {
    if (src == (data_source_t *) successors_only)
      return;
    if ((qn_input_fn) stage_node_input == src->src_input)
      {
	QNCAST (stage_node_t, stnck, src);
	stn_divide_bulk_input (stnck, inst);
	if (!qi->qi_dfg_anytimed)
	  {
	    if ((QST_INT (inst, stnck->clb.clb_fill) || stn_n_input (stnck, inst)) && !SRC_IN_STATE (stnck, inst))
	      GPF_T1 ("stn has fill but not continuable");
	    if (STN_RUN == QST_INT (inst, stnck->stn_state) && !SRC_IN_STATE (stnck, inst))
	      GPF_T1 ("stn in run state but not continuable");
	  }
	if (!stn)
	  {
	    stn = (stage_node_t *) src;
	    sent = QST_INT (inst, stn->stn_out_bytes);
	  }
      }
    else
      {
	if (stn && sent > cl_dfg_batch_bytes)
	  {
	    longjmp_splice (qi->qi_thread->thr_reset_ctx, RST_ENOUGH);
	  }
      }
    if (inst[src->src_in_state])
      {
	*any_done = 1;
	if (src->src_continue_reset)
	  dc_reset_array (inst, src, src->src_continue_reset, -1);
	SRC_RESUME (src, inst);
	src->src_input (src, inst, NULL);
	SRC_RETURN (src, inst);
	goto cont_innermost_loop;
      }
  }
  END_DO_SET ();
  if (subq->qr_cl_run_started)
    inst[subq->qr_cl_run_started] = NULL;
}




int
dfg_is_slice_continuable (stage_node_t * stn, query_instance_t * slice_qi)
{
  dk_set_t nodes =
      local_cll.cll_this_host == QST_INT (slice_qi,
      stn->stn_coordinator_id) ? stn->stn_qf->qf_nodes : stn->src_gen.src_query->qr_nodes;
  DO_SET (data_source_t *, qn, &nodes) if (((caddr_t *) slice_qi)[qn->src_in_state])
    return 1;
  END_DO_SET ();
  return 0;
}


int
cl_dfg_run_local (stage_node_t * stn, caddr_t * inst, caddr_t * err_ret)
{
  /* schedule the ones that are continuable on the aq, wait */
  int inx;
  caddr_t err = NULL, *first_slice = NULL, wait_err;
  QNCAST (QI, main_qi, inst);
  cl_aq_ctx_t claq;
  int64 main_trx_no = main_qi->qi_trx->lt_main_trx_no ? main_qi->qi_trx->lt_main_trx_no : main_qi->qi_trx->lt_trx_no;
  cl_op_t *itcl_clo = QST_BOX (cl_op_t *, inst, stn->stn_qf->clb.clb_itcl->ssl_index);
  itc_cluster_t *_qf_itcl = itcl_clo->_.itcl.itcl;
  cll_in_box_t *rcv_clib = (cll_in_box_t *) _qf_itcl->itcl_clrg->clrg_clibs->next->data;
  async_queue_t *aq = stn_dfg_aq (stn, inst);
  cl_aq_ctx_t auto_claqs[8];
  cl_aq_ctx_t *claqs;
  QI **qis = (QI **) QST_GET_V (inst, stn->stn_slice_qis);
  TMP_ARRAY (QI *, continuable, cont_fill);
  DO_BOX (QI *, slice_qi, inx, qis)
  {
    if (slice_qi)
      stn_divide_bulk_input (stn, (caddr_t *) slice_qi);
    if (slice_qi && dfg_is_slice_continuable (stn, slice_qi))
      {
	slice_qi->qi_threads = 1;
	TMP_ARRAY_ADD (QI *, continuable, cont_fill, slice_qi);
      }
  }
  END_DO_BOX;
  if (!cluster_enable && !cont_fill)
    return 0;
  memzero (&claq, sizeof (claq));
  claq.claq_qr = (query_t *) stn->stn_qf;
  claq.claq_rc_w_id = main_qi->qi_trx->lt_rc_w_id ? main_qi->qi_trx->lt_rc_w_id : main_qi->qi_trx->lt_w_id;
  claq.claq_main_inst = (caddr_t *) main_qi;
  claq.claq_main_trx_no = main_trx_no;
  claq.claq_enlist = (main_qi->qi_trx->lt_cl_branches != NULL) || main_qi->qi_trx->lt_cl_enlisted
      || main_qi->qi_trx->lt_cl_main_enlisted || main_qi->qi_query->qr_need_enlist;
  claq.claq_result_clib = rcv_clib;
  if (cont_fill * sizeof (cl_aq_ctx_t) > sizeof (auto_claqs))
    claqs = (cl_aq_ctx_t *) dk_alloc_box (sizeof (cl_aq_ctx_t) * cont_fill, DV_BIN);
  else
    claqs = auto_claqs;
  for (inx = 0; inx < cont_fill; inx++)
    {
      QI *slice_qi = continuable[inx];
      if (!slice_qi->qi_slice_merits_thread || inx == cont_fill - 1)
	aq->aq_queue_only = 1;
      else
	main_qi->qi_client->cli_activity.da_qp_thread++;
      if (slice_qi->qi_dfg_running)
	GPF_T1 ("restart running dfg slice");
      claqs[inx] = claq;
      aq_request (aq, aq_dfg_local_func, list (2, box_num ((ptrlong) slice_qi), box_num ((ptrlong) & claqs[inx])));
    }
  TMP_ARRAY_DONE (continuable);
  wait_err = NULL;
  aq_wait_all_in_qi (aq, (caddr_t *) main_qi, &wait_err, aq_dfg_cleanup);
  for (inx = 0; inx < cont_fill; inx++)
    if (2 != claqs[inx].claq_of_parent)
      GPF_T1 ("local dfg aq func should have returned");
  if (aq->aq_n_threads || aq->aq_queue.bsk_count)
    GPF_T1 ("aq wait all returns before all done");
  memset (claqs, 0xee, sizeof (cl_aq_ctx_t) * cont_fill);
  if (claqs != auto_claqs)
    dk_free_box ((caddr_t) claqs);
  if (!cluster_enable)
    {
      DO_BOX (QI *, slice_qi, inx, qis)
      {
	if (!slice_qi)
	  continue;
	main_qi->qi_n_affected += slice_qi->qi_n_affected;
	slice_qi->qi_n_affected = 0;
	qi_branch_stats (main_qi, slice_qi, stn->stn_qf->src_gen.src_query);
	if ((err = slice_qi->qi_proc_ret))
	  {
	    *err_ret = slice_qi->qi_proc_ret;
	    slice_qi->qi_proc_ret = NULL;
	  }
      }
      END_DO_BOX;
      return 1;
    }
}



extern resource_t *clib_rc;

void
itcl_dfg_init (itc_cluster_t * itcl, query_frag_t * qf, caddr_t * inst)
{
  /* for the control clrg of a dfg there is one remote clib with one req no.  All final results come with this req no from anywhere.
   * this is also used to send more reqs.  There may be more participant nodes in the query than clibs here.
   * There is exactly one local clo that is used for continuing the local processing.  This clo is never removed, except when the whole dist frag batch is seen to be at end */
  cl_req_group_t *clrg = itcl->itcl_clrg;
  stage_node_t *stn = (stage_node_t *) qf->qf_head_node;
  int inx;
  cll_in_box_t *clib = (cll_in_box_t *) resource_get (clib_rc);
  cl_op_t *clo;
  QNCAST (QI, qi, inst);
  clib->clib_host = cl_id_to_host (local_cll.cll_this_host);
  itcl->itcl_local_when_idle = clib;
  clib->clib_group = clrg;
  clib->clib_waiting = 1;
  clib->clib_vectored = 1;
  clo = mp_clo_allocate (itcl->itcl_pool, CLO_QF_EXEC);
  clo->_.frag.qf = qf;
  clo->_.frag.qst = inst;
  clo->_.frag.is_started = 1;
  basket_add (&clib->clib_local_clo, (void *) clo);
  dk_set_push (&clrg->clrg_clibs, (void *) clib);
  clib = (cll_in_box_t *) resource_get (clib_rc);
  /* any host except this one */
  for (inx = 0; inx <= local_cll.cll_max_host; inx++)
    if ((inx != local_cll.cll_this_host || CL_RUN_LOCAL == cl_run_local_only) && (clib->clib_host = cl_id_to_host (inx)))
      break;
  if (!clib->clib_host)
    GPF_T1 ("dfg does not work if there is only one host");
  clib->clib_group = clrg;
  clib->clib_is_active = 1;
  clib->clib_waiting = 1;
  clib->clib_vectored = 1;
  dk_set_push (&clrg->clrg_clibs, (void *) clib);
  clrg_target_clm (clrg, stn->stn_loc_ts->ts_order_ks->ks_key->key_partition->kpd_map);
  clrg_top_check (clrg, qi_top_qi (qi));
  IN_CLL;
  clib_assign_req_no (clib);
  LEAVE_CLL;
  itcl->itcl_clrg->clrg_is_dfg_rcv = 1;
  qst_set_long (inst, stn->stn_coordinator_req_no, clib->clib_req_no);
  QST_INT (inst, stn->stn_coordinator_id) = local_cll.cll_this_host;
  itcl->itcl_n_clibs = 2;
}
