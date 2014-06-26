

/* single server  dfg, i.e. exchange operator */


#include "aqueue.h"


col_partition_t *
cp_by_ssl (state_slot_t * ssl)
{
  dtp_t dtp = ssl->ssl_sqt.sqt_dtp;
  NEW_VARZ (col_partition_t, cp);
  cp->cp_col_id = 1;
  cp->cp_sqt = ssl->ssl_sqt;
  cp->cp_type = (DV_LONG_INT == dtp || DV_IRI_ID == dtp) ? CP_INT : CP_WORD;
  cp->cp_mask = 0xffffff;
  return cp;
}

key_partition_def_t *
sdfg_key_partition (sql_comp_t * sc, setp_node_t * setp, state_slot_t ** ssl_ret, int *nth_ret)
{
  int inx;
  state_slot_t *best_ssl = NULL;
  float best_score = -1;
  int best_inx = 0;
  for (inx = 0; inx < setp->setp_ha->ha_n_keys; inx++)
    {
      float score;
      state_slot_t *ssl = setp->setp_ha->ha_slots[inx];
      ST *tree = (ST *) sqlg_qn_dfe ((data_source_t *) ssl);
      df_elt_t *col_dfe = sqlo_df (sc->sc_so, tree);
      dtp_t dtp = dtp_canonical[ssl->ssl_sqt.sqt_dtp];
      score = col_dfe ? dfe_exp_card (col_dfe->dfe_sqlo, col_dfe) : 0;
      if (DV_LONG_INT == dtp || DV_IRI_ID == dtp)
	score *= 2;
      if (!best_ssl || score > best_score)
	{
	  best_inx = inx;
	  best_ssl = ssl;
	  best_score = score;
	}
    }

  *nth_ret = best_inx;
  *ssl_ret = best_ssl;
  {
    col_partition_t *cp = cp_by_ssl (best_ssl);
    NEW_VARZ (key_partition_def_t, kpd);
    kpd->kpd_map = clm_all;
    kpd->kpd_cols = dk_alloc_box (sizeof (caddr_t), DV_BIN);
    kpd->kpd_cols[0] = cp_copy (cp, inx);
    kpd->kpd_cols[0]->cp_col_id = best_inx + 1;
    return kpd;
  }
}


void
sdfg_setp_loc_ts (sql_comp_t * sc, setp_node_t * setp)
{
  if (setp->setp_loc_ts)
    return;
  setp->setp_partitioned = 1;
  {
    state_slot_t *part_ssl;
    int nth_ssl;
    key_partition_def_t *kpd = sdfg_key_partition (sc, setp, &part_ssl, &nth_ssl);
    setp->setp_ha->ha_key->key_partition = kpd;
    {
      search_spec_t **prev = NULL;
      dbe_key_t *key = setp->setp_ha->ha_key;
      key_source_t *ks = (key_source_t *) dk_alloc (sizeof (key_source_t));
      SQL_NODE_INIT (table_source_t, ts, table_source_input, ts_free);
      memset (ks, 0, sizeof (key_source_t));
      ts->ts_order_ks = ks;
      ks->ks_key = key;
      prev = &ks->ks_spec.ksp_spec_array;
      {
	NEW_VARZ (search_spec_t, sp);
	*prev = sp;
	prev = &sp->sp_next;
	sp->sp_min_op = CMP_EQ;
	sp->sp_col = (dbe_column_t *) dk_set_nth (key->key_parts, nth_ssl);
	sp->sp_cl = *key_find_cl (key, sp->sp_col->col_id);
	sp->sp_min_ssl = part_ssl;
      }

      setp->setp_loc_ts = ts;
    }
  }
}


void
sdfg_hs_loc_ts (sql_comp_t * sc, hash_source_t * hs)
{
  /* a right oj hs is always partitioned.  The part key is the same as that of the filler */
  setp_node_t *setp = hs->hs_filler->fnr_setp;
  key_source_t *ks = setp->setp_loc_ts->ts_order_ks;
  state_slot_t *part_ssl = ks->ks_spec.ksp_spec_array->sp_min_ssl, *hs_part_ssl;
  int nth_part = 0;
  hs->hs_cl_partition = HS_CL_PART;
  DO_SET (state_slot_t *, key_ssl, &setp->setp_keys)
  {
    if (key_ssl == part_ssl)
      break;
    nth_part++;

  }
  END_DO_SET ();
  hs_part_ssl = hs->hs_ref_slots[nth_part];
  hs->hs_ha->ha_key->key_partition = kpd_copy (ks->ks_key->key_partition);
  {
    search_spec_t **prev = NULL;
    dbe_key_t *key = hs->hs_ha->ha_key;
    key_source_t *ks = (key_source_t *) dk_alloc (sizeof (key_source_t));
    SQL_NODE_INIT (table_source_t, ts, table_source_input, ts_free);
    memset (ks, 0, sizeof (key_source_t));
    ts->ts_order_ks = ks;
    ks->ks_key = key;
    prev = &ks->ks_spec.ksp_spec_array;
    {
      NEW_VARZ (search_spec_t, sp);
      *prev = sp;
      prev = &sp->sp_next;
      sp->sp_min_op = CMP_EQ;
      sp->sp_col = (dbe_column_t *) dk_set_nth (key->key_parts, nth_part);
      sp->sp_cl = *key_find_cl (key, sp->sp_col->col_id);
      sp->sp_min_ssl = hs_part_ssl;
    }
    hs->hs_loc_ts = ts;
  }
}

int
sqlg_sdfg_colocated (sql_comp_t * sc, setp_node_t * setp)
{
  /* in single server a high card group by may have a grouping col that is equal to the partitioning col of the previous sdfg stage.  If so, the setp is colocated, no sdfg stage in front */
  stage_node_t *stn;
  state_slot_t *part_ssl;
  if (!sc->sc_dfg_stages)
    return 0;
  stn = (stage_node_t *) sc->sc_dfg_stages->data;
  part_ssl = stn->stn_loc_ts->ts_order_ks->ks_spec.ksp_spec_array->sp_min_ssl;
  DO_SET (state_slot_t *, g_ssl, &setp->setp_keys)
  {
    if (ssl_is_eq (sc, g_ssl, part_ssl))
      {
	setp->setp_partitioned = 1;
	return 1;
      }
  }
  END_DO_SET ();
  return 0;
}


data_source_t *
qn_next_qn (data_source_t * ts, qn_input_fn in)
{
  for (ts = ts; ts; ts = qn_next (ts))
    if (IS_QN (ts, in))
      return ts;
  return NULL;
}


stage_node_t *
qn_next_stn (data_source_t * ts)
{
  for (ts = ts; ts; ts = qn_next (ts))
    if (IS_QN (ts, stage_node_input))
      return (stage_node_t *) ts;
  return NULL;
}


void
ts_sdfg_run (table_source_t * ts, caddr_t * inst)
{
  /* ts is outermost ts of sdfg.  Wait for aq, feed until all done */
  stage_node_t stn_auto;
  QNCAST (QI, qi, inst);
  stage_node_t *stn = qn_next_stn ((data_source_t *) ts);
  cl_op_t *itcl_clo = (cl_op_t *) qst_get (inst, ts->clb.clb_itcl);
  cl_req_group_t *clrg = itcl_clo->_.itcl.itcl->itcl_clrg;
  cll_in_box_t *rcv_clib = (cll_in_box_t *) clrg->clrg_clibs->data;
  int save_sl = qi->qi_is_dfg_slice;
  caddr_t **qis = QST_BOX (caddr_t **, inst, ts->ts_aq_qis->ssl_index);
  if ((!IS_TS (ts) && !IS_QN (ts, chash_read_input)) || !ts->ts_aq_state)
    GPF_T1 ("in ts sdfg run, the ts is not a ts or chash reader or does not have ts aq state");
  if (!stn)
    stn = (stage_node_t *) qf_first_qn (ts->ts_qf, (qn_input_fn) stage_node_input);
  qi->qi_is_dfg_slice = 0;
  QST_INT (inst, ts->ts_aq_state) = TS_AQ_COORD;
  if (stn->src_gen.src_query != ts->src_gen.src_query)
    {
      stn_auto = *stn;
      stn_auto.src_gen.src_query = ts->src_gen.src_query;
      stn = &stn_auto;
    }
  for (;;)
    {
      caddr_t err = NULL;
      int rc, inx;
      /* reset the out bytes between feeding of the local dfg.  All out bytes are consumed by the feed */
      DO_BOX (caddr_t *, slice_qi, inx, qis)
      {
	if (slice_qi)
	  {
	    qst_set_long (slice_qi, stn->stn_coordinator_req_no, rcv_clib->clib_req_no);
	    QST_INT (slice_qi, stn->stn_out_bytes) = 0;
	  }
      }
      END_DO_BOX;
      IN_CLL;
      mutex_enter (&clrg->clrg_mtx);
      dfg_feed (stn, inst, &rcv_clib->clib_in);
      LEAVE_CLL;
      mutex_leave (&clrg->clrg_mtx);
      dfg_after_feed ();
      rc = cl_dfg_run_local (stn, inst, &err);
      if (err)
	{
	  qi->qi_is_dfg_slice = save_sl;
	  sqlr_resignal (err);
	}
      if (!rc)
	break;
    }
  qi->qi_is_dfg_slice = save_sl;
  QST_INT (inst, ts->ts_aq_state) = TS_AQ_NONE;
}

void itcl_dfg_init (itc_cluster_t * itcl, query_frag_t * qf, caddr_t * inst);


void
ts_sdfg_init (table_source_t * ts, caddr_t * inst)
{
  /* Before first parallel ts of a sdfg */
  cll_in_box_t *rcv_clib;
  QNCAST (QI, qi, inst);
  stage_node_t *stn = qn_next_stn ((data_source_t *) ts);
  itc_cluster_t *itcl = itcl_allocate (qi->qi_trx, inst);
  cl_op_t *itcl_clo = clo_allocate (CLO_ITCL);
  itcl_clo->_.itcl.itcl = itcl;
  qst_set (inst, ts->clb.clb_itcl, (caddr_t) itcl_clo);
  itcl_dfg_init (itcl, stn->stn_qf, inst);
  rcv_clib = (cll_in_box_t *) itcl->itcl_clrg->clrg_clibs->data;
  rcv_clib->clib_local_pool = mem_pool_alloc ();
}

void
ts_sliced_reader (table_source_t * ts, caddr_t * inst, hash_area_t * ha)
{
  /* processing high card partitioned gby results, thread per partition.  If ends in a oby or non partitioned gby, add up at the end. */
  index_tree_t **slice_trees;
  QNCAST (QI, qi, inst);
  stage_node_t *stn = (stage_node_t *) qf_first_qn (ts->ts_qf, (qn_input_fn) stage_node_input);
  fun_ref_node_t *fref;
  caddr_t **qis = QST_BOX (caddr_t **, inst, ts->ts_aq_qis->ssl_index);
  int inx, n_branches = 0;
  index_tree_t *tree = (index_tree_t *) qst_get (inst, ha->ha_tree);
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill);
  if (!tree || !tree->it_hi || !(slice_trees = tree->it_hi->hi_slice_trees))
    return;
  if (!qis && tree && tree->it_hi->hi_slice_trees)
    {
      qis = (caddr_t **) dk_alloc_box_zero (box_length (tree->it_hi->hi_slice_trees), DV_ARRAY_OF_POINTER);
      QST_BOX (caddr_t **, inst, ts->ts_aq_qis->ssl_index) = qis;
    }
  DO_BOX (caddr_t *, slice_inst, inx, qis)
  {
    if (slice_inst)
      {
	QNCAST (QI, slice_qi, slice_inst);
	if (slice_qi->qi_slice >= clm_all->clm_distinct_slices)
	  continue;
	QST_INT (slice_inst, ts->ts_in_sdfg) = 1;
	SRC_IN_STATE (ts, slice_inst) = slice_inst;
	n_branches++;
      }
    else if (inx < BOX_ELEMENTS (slice_trees) && slice_trees[inx] && slice_trees[inx] != tree)
      {
	/* need slice qi for reading a slice tree for which there is no qi now since the qi was not present on the last batch */
	caddr_t *slice_inst = qf_slice_qi (stn->stn_qf, inst, 0);;
	QNCAST (QI, slice_qi, slice_inst);
	qis[inx] = slice_inst;
	slice_qi->qi_slice = inx;
	qst_set (slice_inst, ha->ha_tree, box_copy ((caddr_t) slice_trees[inx]));
	QST_INT (slice_inst, ts->ts_in_sdfg) = 1;
	SRC_IN_STATE (ts, slice_inst) = slice_inst;
	n_branches++;
      }
  }
  END_DO_BOX;
  DO_BOX (state_slot_t *, ssl, inx, ts->ts_sdfg_params)
  {
    int sinx, set;
    DO_BOX (QI *, slice_qi, sinx, qis)
    {
      if (!slice_qi)
	continue;
      for (set = 0; set < n_sets; set++)
	{
	  qi->qi_set = set;
	  slice_qi->qi_set = set;
	  qst_set_copy ((caddr_t *) slice_qi, ssl, qst_get (inst, ts->ts_sdfg_param_refs[inx]));
	  slice_qi->qi_set = 0;
	}
    }
    END_DO_BOX;
  }
  END_DO_BOX;
  qi->qi_set = 0;
  ts_sdfg_run (ts, inst);
  fref = (fun_ref_node_t *) ts->ts_agg_node;
  if (fref && fref->fnr_setp && fref->fnr_setp->setp_partitioned)
    return;
  ts_aq_result (ts, inst);
  qi_inc_branch_count (qi, 0, 1 - n_branches);
}
