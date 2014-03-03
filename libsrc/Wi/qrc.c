

#include "libutil.h"

#include "sqlnode.h"
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "sqlbif.h"
#include "arith.h"
#include "security.h"
#include "sqlo.h"
#include "mhash.h"
#include "rdfinf.h"


id_hash_t *qr_cache;
dk_mutex_t qrc_mtx;
int enable_qrc = 0;
long tc_qrc_hit;
long tc_qrc_miss;
long tc_qrc_recompile;
long tc_qrc_plan_miss;


typedef struct qc_key_s
{
  uint64 qck_hash;
  ST *qck_tree;
} qc_key_t;




uint32
qck_hash (caddr_t e)
{
  return ((qc_key_t *) e)->qck_hash & 0x7fffffff;
}


int
qck_tree_cmp (ST * k1, ST * k2)
{
  int l1, l2, inx;
  dtp_t dtp1 = DV_TYPE_OF (k1);
  dtp_t dtp2 = DV_TYPE_OF (k2);
  if (k1 == k2)
    return 1;
  if (dtp1 != dtp2)
    return 0;
  if (!IS_BOX_POINTER (k1) || !IS_BOX_POINTER (k2))
    return k1 == k2;
  l1 = box_length (k1);
  l2 = box_length (k2);
  if (l1 != l2)
    return 0;
  if (DV_ARRAY_OF_POINTER == dtp1)
    {
      if (ST_P (k1, LIT_PARAM) && ST_P (k2, LIT_PARAM))
	return k1->_.lit_param.nth == k2->_.lit_param.nth;
      DO_BOX (ST *, s1, inx, k1) if (!qck_tree_cmp (s1, ((ST **) k2)[inx]))
	return 0;
      END_DO_BOX;
      return 1;
    }
  return box_equal ((caddr_t) k1, (caddr_t) k2);
}


int
qck_cmp (qc_key_t ** e1, qc_key_t ** e2)
{
  qc_key_t *k1 = (qc_key_t *) e1;
  qc_key_t *k2 = (qc_key_t *) e2;
  if (k1->qck_hash != k2->qck_hash)
    return 0;
  return qck_tree_cmp (k1->qck_tree, k2->qck_tree);
}







void
sqlo_lit_param (sqlo_t * so, ST ** plit)
{
  ST *lit;
  int64 *place, nn;
  st_lit_state_t *stl = so->so_stl;
  if (!stl || stl->stl_no_record)
    return;
  lit = *plit;
  place = (int64 *) id_hash_get (stl->stl_tree_to_lit, (caddr_t) & lit);
  if (!place)
    {
      if (stl->stl_lit_ctr > 100)
	return;
      nn = ++stl->stl_lit_ctr;
      t_id_hash_set (stl->stl_tree_to_lit, (caddr_t) & lit, (caddr_t) & nn);
    }
  else
    nn = *place;
  *plit = t_listst (3, LIT_PARAM, nn, lit);
}


extern caddr_t rdfs_type;


void
sqlo_check_rhs_lit (sqlo_t * so, ST * tree)
{
  st_lit_state_t *stl = so->so_stl;
  if (!stl)
    {
      sqlo_scope (so, &tree->_.bin_exp.right);
      return;
    }
  switch (tree->type)
    {
    case BOP_EQ:
      if (ST_P (tree->_.bin_exp.left, COL_DOTTED)
	  && !stricmp ("p", tree->_.bin_exp.left->_.col_ref.name) && tree->_.bin_exp.left->_.col_ref.prefix)
	{
	  stl->stl_no_record = 1;
	  sqlo_scope (so, &tree->_.bin_exp.right);
	  if (box_equal (rdfs_type, tree->_.bin_exp.right))
	    t_set_push (&stl->stl_type_cnos, (void *) (ptrlong) OT_NO (tree->_.bin_exp.left->_.col_ref.prefix));
	  stl->stl_no_record = 0;
	  return;
	}
      else if (ST_P (tree->_.bin_exp.left, COL_DOTTED)
	  && !stricmp ("o", tree->_.bin_exp.left->_.col_ref.name) && tree->_.bin_exp.left->_.col_ref.prefix)
	{
	  int cno = OT_NO (tree->_.bin_exp.left->_.col_ref.prefix);
	  if (dk_set_member (stl->stl_type_cnos, (void *) (ptrlong) cno))
	    stl->stl_no_record = 1;
	  sqlo_scope (so, &tree->_.bin_exp.right);
	  stl->stl_no_record = 0;
	}
      else
	sqlo_scope (so, &tree->_.bin_exp.right);
      break;
    default:
      sqlo_scope (so, &tree->_.bin_exp.right);
    }
}

uint32
qces_hash (qce_sample_t ** qces)
{
  return treehash ((caddr_t) & (*qces)->qces_sc_key);
}

int
qces_cmp (qce_sample_t ** ps1, qce_sample_t ** ps2)
{
  qce_sample_t *s1 = *ps1;
  qce_sample_t *s2 = *ps2;
  return box_equal (s1->qces_sc_key, s2->qces_sc_key)
      && s1->qces_n_params == s2->qces_n_params
      && 0 == memcmp (&s1->qces_params, s2->qces_params, s1->qces_n_params * sizeof (int32));
}


void
sqlo_init_lit_params (sql_comp_t * sc, sqlo_t * so)
{
  if (!enable_qrc || !qr_cache)
    return;
  if (sc->sc_super)
    {
      query_t *top_qr = top_sc->sc_cc->cc_query;
      return;
      if (top_qr->qr_proc_name || top_qr->qr_trig_event)
	return;
    }
  t_NEW_VARZ (st_lit_state_t, stl);
  stl->stl_tree_to_lit = t_id_hash_allocate (11, sizeof (caddr_t), sizeof (ptrlong), treehash, treehashcmp);
  stl->stl_samples = t_id_hash_allocate (11, sizeof (qce_sample_t), 0, (hash_func_t) qces_hash, (cmp_func_t) qces_cmp);
  so->so_stl = stl;
  for (sc = sc; sc; sc = sc->sc_super)
    sc->sc_stl = stl;
}


id_hashed_key_t
sql_tree_hash_tpl (ST * st)
{
  dtp_t dtp;
  if (!IS_BOX_POINTER (st))
    return (uint32) (ptrlong) st;
  dtp = box_tag (st);
  switch (dtp)
    {
    case DV_LONG_INT:
      return (uint32) * (boxint *) st;
    case DV_STRING:
    case DV_C_STRING:
    case DV_SYMBOL:
    case DV_UNAME:
      {
	char *str = (char *) st;
	int len = box_length ((caddr_t) st) - 1;
	uint32 hash = 1234567;
	if (len > 10)
	  {
	    int d = len / 2;
	    d &= ~7L;
	    len -= d;
	    str += d;
	  }
	/*BYTE_BUFFER_HASH (hash, str, len); */
	MHASH_VAR (hash, str, len);
	return hash;
      }
    case DV_ARRAY_OF_POINTER:
      {
	int len = BOX_ELEMENTS (st);
	uptrlong first;
	uint32 hash, inx;
	if (!len)
	  return 1;
	first = st->type;
	if (LIT_PARAM == first)
	  return 1;
	if (first < 10000)
	  hash = first;
	else
	  hash = sql_tree_hash_tpl ((ST *) first);
	if (SELECT_STMT == first && len > 4 && DV_ARRAY_OF_POINTER == DV_TYPE_OF (st->_.select_stmt.selection))
	  {
	    return sql_tree_hash_tpl ((ST *) st->_.select_stmt.selection);
	  }
	if (len > 5)
	  len = 5;
	for (inx = 1; inx < len; inx++)
	  hash = ((hash >> 2) | ((hash & 3 << 30))) ^ sql_tree_hash_tpl (((ST **) st)[inx]);
	return hash;
      }
    default:
      return box_hash ((caddr_t) st);
    }
}


void
qrc_set_lit_params (sql_comp_t * sc, query_t * qr)
{
  du_thread_t *self = THREAD_CURRENT_THREAD;
  st_lit_state_t *stl = sc->sc_stl;
  caddr_t *params = (caddr_t *) dk_alloc_box (sizeof (caddr_t) * stl->stl_tree_to_lit->ht_count, DV_BIN);
  DO_IDHASH (caddr_t, lit, ptrlong, inx, stl->stl_tree_to_lit) params[inx - 1] = box_copy_tree (lit);
  END_DO_IDHASH;
  SET_THR_ATTR (self, TA_QRC_LIT_PARAMS, params);
}


qce_sample_t **
qrc_samples (st_lit_state_t * stl, query_t * qr)
{
  int fill = 0;
  qce_sample_t **s = (qce_sample_t **) dk_alloc_box (sizeof (caddr_t) * stl->stl_samples->ht_count, DV_BIN);
  DO_IDHASH (qce_sample_t *, qces, void *, ign, stl->stl_samples)
  {
    s[fill++] = qces;
  }
  END_DO_IDHASH;
  return s;
}

int32 qrc_tolerance = 20;

int
qrc_card_in_range (float c1, float c2)
{
  if (c1 == c2)
    return 1;
  if (0 == c1 || 0 == c2)
    return 0;
  return c1 / c2 < ((100 + qrc_tolerance) / 100.0) && c1 / c2 > 1 / ((100 + qrc_tolerance) / 100.0);
}

df_elt_t *
dfe_col_const_cmp (sqlo_t * so, dbe_column_t * col, int op, caddr_t cnst)
{
  df_elt_t *cmp = sqlo_new_dfe (so, DFE_BOP_PRED, NULL);
  cmp->_.bin.op = dvc_to_bop (op);
  df_elt_t *cd = sqlo_new_dfe (so, DFE_CONST, NULL);
  cd->dfe_tree = cnst;
  df_elt_t *lf = sqlo_new_dfe (so, DFE_COLUMN, NULL);
  lf->_.col.col = col;
  cmp->_.bin.left = lf;
  cmp->_.bin.right = cd;
  return cmp;
}

void
dfe_key_from_sck (df_elt_t * tb_dfe, dtp_t ops, caddr_t * sc_key, df_elt_t ** lower, df_elt_t ** upper, int inx, int *param_ctr)
{
  sqlo_t *so = tb_dfe->dfe_sqlo;
  dtp_t min = ops & 0xf;
  dtp_t max = ops >> 4;
  dbe_key_t *key = tb_dfe->_.table.key;
  if (CMP_NONE != min)
    {
      lower[inx] = dfe_col_const_cmp (so, dk_set_nth (key->key_parts, inx), min, sc_key[*param_ctr + 1]);
      (*param_ctr)++;
    }
  if (CMP_NONE != max)
    {
      upper[inx] = dfe_col_const_cmp (so, dk_set_nth (key->key_parts, inx), max, sc_key[*param_ctr + 1]);
      (*param_ctr)++;
    }
  if (!lower[inx] && upper[inx])
    {
      lower[inx] = upper[inx];
      upper[inx] = NULL;
    }
}


void
dfe_dep_to_spec (df_elt_t * tb_dfe, oid_t col_id, dtp_t min, dtp_t max, caddr_t * sc_key, int *param_ctr)
{
  dbe_column_t *col = sch_id_to_column (wi_inst.wi_schema, col_id);
  df_elt_t *dfe = dfe_col_const_cmp (tb_dfe->dfe_sqlo, col, CMP_NONE != min ? min : max, sc_key[*param_ctr + 1]);
  (*param_ctr)++;
  t_set_push (&tb_dfe->_.table.col_preds, (void *) dfe);
}


float
qrc_sample_sc_key (sqlo_t * so, qce_sample_t * qces, caddr_t * sc_key)
{
  op_table_t ot;
  df_elt_t tb_auto;
  int param_ctr = 0, fill = 0;
  df_elt_t *tb_dfe = &tb_auto;
  index_choice_t ic;
  int inx;
  memzero (&ot, sizeof (ot));
  memzero (&tb_auto, sizeof (tb_auto));
  df_elt_t *lower[10];
  df_elt_t *upper[10];
  sample_cache_key_t *sck = (sample_cache_key_t *) sc_key[0];
  dbe_key_t *key = sch_id_to_key (wi_inst.wi_schema, sck->sck_key);
  memzero (&ic, sizeof (ic));
  memzero (lower, sizeof (lower));
  memzero (upper, sizeof (upper));
  ic.ic_key = key;
  tb_dfe->_.table.ot = &ot;
  ot.ot_table = key->key_table;
  tb_dfe->_.table.key = key;
  tb_dfe->dfe_sqlo = so;
  ic.ic_ric = qces->qces_ric;
  for (inx = 0; inx < sck->sck_n_key; inx++)
    {
      dfe_key_from_sck (tb_dfe, sck->sck_data[inx], sc_key, lower, upper, inx, &param_ctr);
    }
  fill = sck->sck_n_key;
  for (inx = 0; inx < sck->sck_n_dep; inx++)
    {
      int col_id = LONG_REF_NA (&sck->sck_data[fill]);
      int min = sck->sck_data[fill + 4];
      int max = sck->sck_data[fill + 5];
      dfe_dep_to_spec (tb_dfe, col_id, min, max, sc_key, &param_ctr);
      fill += 6;
    }
  tb_dfe->dfe_locus = LOC_LOCAL;
  return sqlo_inx_sample (tb_dfe, key, lower, upper, sck->sck_n_key, &ic);
}


int qrc_sample_compatible (sqlo_t * so, qce_sample_t * qces, caddr_t * lits)
{
  /* take the sample in qces with the lits in stl and see if close enough */
  float smp = -1;
  tb_sample_t *place;
  caddr_t *sc_key = (caddr_t *) t_box_copy_tree (qces->qces_sc_key);
  int inx;
  for (inx = 0; inx < qces->qces_n_params; inx++)
    {
      int pos = 1 + (qces->qces_params[inx] >> 16);
      int nth = qces->qces_params[inx] & 0xffff;
      caddr_t lit = lits[nth - 1];
      if (box_equal (lit, sc_key[pos]))
	if (inx == qces->qces_n_params - 1)
	  {
	    /* all eq, no resample */
	    return 1;
	  }
      sc_key[pos] = lit;
    }
  mutex_enter (qces->qces_ric->ric_mtx);
  place = (tb_sample_t *) id_hash_get (qces->qces_ric->ric_samples, (caddr_t) & sc_key);
  if (place)
    smp = place->smp_card;
  mutex_leave (qces->qces_ric->ric_mtx);
  if (-1 != smp)
    return qrc_card_in_range (smp, qces->qces_card);
  smp = qrc_sample_sc_key (so, qces, sc_key);
  return qrc_card_in_range (smp, qces->qces_card);
}


query_t *
qrc_best_qr (sqlo_t * so, qc_data_t * qcd)
{
  du_thread_t *self = THREAD_CURRENT_THREAD;
  caddr_t *lits = (caddr_t *) THR_ATTR (self, TA_QRC_LIT_PARAMS);
  qr_cache_ent_t *qce;
  int inx, inx2;
  DO_BOX (query_t *, qr1, inx, qcd->qcd_queries)
  {
    if (!qr1)
      continue;
    if (qr1->qr_to_recompile)
      {
	qcd->qcd_to_drop = 1;
	return NULL;
      }
    qce = qr1->qr_qce;
    DO_BOX (qce_sample_t *, qces, inx2, qce->qce_samples)
    {
      if (!qrc_sample_compatible (so, qces, lits))
	goto next;
    }
    END_DO_BOX;
    mutex_enter (&qrc_mtx);
    qce->qce_ref_count++;
    qce->qce_n_used++;
    qce->qce_last_used = approx_msec_real_time ();
    mutex_leave (&qrc_mtx);
    return qr1;
  next:;
  }
  END_DO_BOX;
  return NULL;
}


void
qcd_unref (qc_data_t * qcd, int is_in_qrc)
{
  int inx;
  if (!is_in_qrc)
    mutex_enter (&qrc_mtx);
  qcd->qcd_ref_count--;
  if (qcd->qcd_to_drop && 0 == qcd->qcd_ref_count)
    {
      mutex_leave (&qrc_mtx);
      DO_BOX (query_t *, qr, inx, qcd->qcd_queries) qr_free (qr);
      END_DO_BOX;
      DO_SET (query_t *, qr, &qcd->qcd_to_add) qr_free (qr);
      END_DO_SET ();
      dk_free_box ((caddr_t) qcd->qcd_queries);
      dk_free ((caddr_t) qcd, sizeof (qc_data_t));
      return;
    }
  if (!qcd->qcd_ref_count)
    {
      DO_SET (query_t *, qr, &qcd->qcd_to_add)
      {
	qcd->qcd_queries = (query_t **) box_append_1 ((caddr_t) qcd->qcd_queries, (caddr_t) qr);
      }
      END_DO_SET ();
      dk_set_free (qcd->qcd_to_add);
      qcd->qcd_to_add = NULL;
    }
  mutex_leave (&qrc_mtx);
}


void
qrc_lookup (sql_comp_t * sc, ST * tree)
{
  query_t *qr1 = NULL, *one_qr = NULL, *best_qr;
  qc_data_t **place;
  qc_data_t *qcd;
  qc_key_t k;
  int inx;
  jmp_buf_splice *ctx;
  st_lit_state_t *stl = sc->sc_stl;
  if (!stl || stl->stl_explicit_params)
    {
      sc->sc_so->so_stl = sc->sc_stl = NULL;
      return;
    }
  k.qck_hash = sql_tree_hash_tpl (tree);
  k.qck_tree = tree;
  stl->stl_hash = k.qck_hash;
  stl->stl_tree = tree;
  qrc_set_lit_params (sc, qr1);
  mutex_enter (&qrc_mtx);
  place = (qc_data_t **) id_hash_get (qr_cache, (caddr_t) & k);
  if (!place)
    {
      mutex_leave (&qrc_mtx);
      stl->stl_tree = box_copy_tree ((caddr_t) stl->stl_tree);
      TC (tc_qrc_miss);
      return;
    }
  qcd = *place;
  qcd->qcd_ref_count++;
  DO_BOX (query_t *, qr1, inx, qcd->qcd_queries)
  {
    if (!qr1)
      continue;
    one_qr = qr1;
    if (qr1->qr_to_recompile)
      {
	id_hash_remove (qr_cache, (caddr_t) & k);
	qcd->qcd_to_drop = 1;
	qcd_unref (qcd, 1);
	TC (tc_qrc_recompile);
	return;
      }
  }
  END_DO_BOX;
  mutex_leave (&qrc_mtx);
  if (!one_qr)
    {
      TC (tc_qrc_miss);
      return;
    }
  best_qr = qrc_best_qr (sc->sc_so, qcd);
  qcd_unref (qcd, 0);
  if (!best_qr)
    {
      stl->stl_tree = (ST *) box_copy_tree ((caddr_t) stl->stl_tree);
      TC (tc_qrc_plan_miss);
      return;
    }
  TC (tc_qrc_hit);
  sc->sc_cc->cc_query = best_qr;
  ctx = (jmp_buf_splice *) THR_ATTR (THREAD_CURRENT_THREAD, CATCH_LISP_ERROR);
  longjmp_splice (ctx, QRC_FOUND);
}

void
qr_set_qce (query_t * qr, qc_data_t * qcd, qce_sample_t ** samples)
{
  qr_cache_ent_t *qce = (qr_cache_ent_t *) dk_alloc (sizeof (qr_cache_ent_t));
  memzero (qce, sizeof (qr_cache_ent_t));
  qce->qce_samples = samples;
  qr->qr_qce = qce;
  qce->qce_qcd = qcd;
  qce->qce_ref_count = 1;
}


int
qr_same_cards (query_t * qr1, query_t * qr2)
{
  /* if two compilations are made concurrently for the same card ballpark, keep just one */
  return 0;
}


int
qcd_add_if_new (qc_data_t * qcd, query_t * new_qr, qce_sample_t ** samples)
{
  int inx;
  query_t **new_qrs = (query_t **) dk_set_to_array (qcd->qcd_to_add);
  mutex_leave (&qrc_mtx);
  DO_BOX (query_t *, qr, inx, new_qrs)
  {
    if (qr_same_cards (qr, new_qr))
      {
	dk_free_box ((caddr_t) new_qrs);
	return 0;
      }
  }
  END_DO_BOX;
  dk_free_box ((caddr_t) new_qrs);
  DO_BOX (query_t *, qr, inx, qcd->qcd_queries)
  {
    if (qr_same_cards (qr, new_qr))
      return 0;
  }
  END_DO_BOX;

  qr_set_qce (new_qr, qcd, samples);
  mutex_enter (&qrc_mtx);
  if (1 == qcd->qcd_ref_count)
    qcd->qcd_queries = (query_t **) box_append_1 ((caddr_t) qcd->qcd_queries, (caddr_t) new_qr);
  else
    dk_set_push (&qcd->qcd_to_add, (void *) new_qr);
  mutex_leave (&qrc_mtx);
  return 1;
}


void
qces_arr_free (qce_sample_t ** qces_arr)
{
  int inx;
  DO_BOX (qce_sample_t *, qces, inx, qces_arr)
  {
    dk_free_tree (qces->qces_sc_key);
    dk_free_box ((caddr_t) qces);
  }
  END_DO_BOX;
  dk_free_box ((caddr_t) qces_arr);
}


void
qce_free (qr_cache_ent_t * qce)
{
  qces_arr_free (qce->qce_samples);
  dk_free ((caddr_t) qce, sizeof (qr_cache_ent_t));
}



void
qrc_set (sql_comp_t * sc, query_t * qr)
{
  st_lit_state_t *stl = sc->sc_stl;
  qc_key_t qck;
  qc_data_t *qcd;
  qc_data_t **place;
  qr_cache_ent_t *qce;
  qce_sample_t **samples;
  if (!sc->sc_stl)
    return;
  samples = qrc_samples (sc->sc_stl, qr);
  qck.qck_hash = stl->stl_hash;
  qck.qck_tree = stl->stl_tree;
  mutex_enter (&qrc_mtx);
  place = (qc_data_t *) id_hash_get (qr_cache, (caddr_t) & qck);
  if (place)
    {
      qcd = *place;
      qcd->qcd_ref_count++;
      if (!qcd_add_if_new (qcd, qr, samples))
	{
	  qces_arr_free (samples);
	  qcd_unref (qcd, 0);
	  return;
	}
      return;
    }
  mutex_leave (&qrc_mtx);
  qck.qck_tree = stl->stl_tree;
  qcd = (qc_data_t *) dk_alloc (sizeof (qc_data_t));
  memzero (qcd, sizeof (qc_data_t));
  qcd->qcd_tree = (caddr_t) qck.qck_tree;
  qcd->qcd_queries = dk_alloc_box_zero (1 * sizeof (caddr_t), DV_BIN);
  qcd->qcd_queries[0] = qr;
  qr_set_qce (qr, qcd, samples);
  mutex_enter (&qrc_mtx);
  place = (qc_data_t *) id_hash_get (qr_cache, (caddr_t) & qck);
  if (place)
    {
      mutex_leave (&qrc_mtx);
      qce_free (qr->qr_qce);
      qr->qr_qce = NULL;
      return;
    }
  id_hash_set (qr_cache, (caddr_t) & qck, (caddr_t) & qcd);
  mutex_leave (&qrc_mtx);
}


int
qrc_free (query_t * qr)
{
  if (!qr->qr_qce)
    return 1;
  mutex_enter (&qrc_mtx);
  qr->qr_qce->qce_ref_count--;
  if ((qr->qr_to_recompile || qr->qr_qce->qce_free_when_done) && 0 == qr->qr_qce->qce_ref_count)
    {
      qrc_remove (qr, 1);
      mutex_leave (&qrc_mtx);
      qce_free (qr->qr_qce);
      qr->qr_qce = NULL;
      return 1;
    }
  mutex_leave (&qrc_mtx);
  return 0;
}

void
qrc_remove (query_t * qr, int is_in_qrc)
{
  int inx;
  qc_data_t *qcd;
  int any = 0;
  qr_cache_ent_t *qce = NULL;
  if (!is_in_qrc)
    mutex_enter (&qrc_mtx);
  qcd = qr->qr_qce->qce_qcd;
  if (!qcd)
    {
      if (!is_in_qrc)
	mutex_leave (&qrc_mtx);
      return;
    }
  for (inx = 0; inx < qcd->qcd_n_queries; inx++)
    {
      if (qcd->qcd_queries[inx] == qr)
	{
	  qcd->qcd_queries[inx] = NULL;
	  qce = qr->qr_qce;
	  qce->qce_free_when_done = 1;
	}
      if (qcd->qcd_queries[inx])
	any = 1;
    }
  if (!any)
    {
      qc_key_t k;
      k.qck_tree = qcd->qcd_tree;
      id_hash_remove (qr_cache, (caddr_t) & k);
    }
  if (!is_in_qrc)
    mutex_leave (&qrc_mtx);
  if (!any)
    {
      dk_free_tree (qcd->qcd_tree);
      dk_free ((caddr_t) qcd, sizeof (qc_data_t));
    }
}


caddr_t
bif_qrc_clear (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  id_hash_iterator_t hit;
  qc_data_t **pqcd, *qcd;
  qc_key_t *qck;
  dk_set_t qck_to_free = NULL;
  dk_set_t qcd_to_free = NULL;
  mutex_enter (&qrc_mtx);
  id_hash_iterator (&hit, qr_cache);
  while (hit_next (&hit, (caddr_t *) & qck, (caddr_t *) & pqcd))
    {
      qcd = *pqcd;
      if (qcd->qcd_ref_count)
	continue;
      dk_set_push (&qck_to_free, (void *) qck);
      dk_set_push (&qcd_to_free, (void *) qcd);
    }
  DO_SET (qc_key_t *, qck, &qck_to_free) id_hash_remove (qr_cache, (caddr_t) qck);
  END_DO_SET ();
  mutex_leave (&qrc_mtx);
  dk_set_free (qck_to_free);
  DO_SET (qc_data_t *, qcd, &qcd_to_free)
  {
    int inx;
    DO_BOX (query_t *, qr, inx, qcd->qcd_queries)
    {
      int left = 0;
      if (!qr)
	continue;
      mutex_enter (&qrc_mtx);
      if (qr->qr_qce)
	{
	  if (!qr->qr_qce->qce_ref_count)
	    {
	      mutex_leave (&qrc_mtx);
	      left = 1;
	      qce_free (qr->qr_qce);
	      qr->qr_qce = NULL;
	      qr_free (qr);
	    }
	  else
	    {
	      qr->qr_qce->qce_free_when_done = 1;
	      qr->qr_qce->qce_qcd = NULL;
	    }
	}
      if (!left)
	mutex_leave (&qrc_mtx);
    }
    END_DO_BOX;
    dk_free_tree (qcd->qcd_tree);
    dk_free_box (qcd->qcd_queries);
    dk_free ((caddr_t) qcd, sizeof (qc_data_t));
  }
  END_DO_SET ();
  dk_set_free (qcd_to_free);
  return NULL;
}


void
qrc_status ()
{
  uint32 now = get_msec_real_time ();
  id_hash_iterator_t hit;
  qc_data_t **pqcd, *qcd;
  qc_key_t *qck;
  trset_printf
      ("Query cache:  %Ld hits %Ld misses %Ld hit but new plan made due to different cardinalities  %Ld recompile\n%d query templates",
      tc_qrc_miss, tc_qrc_miss, tc_qrc_plan_miss, tc_qrc_recompile, qr_cache->ht_count);
  mutex_enter (&qrc_mtx);
  id_hash_iterator (&hit, qr_cache);
  while (hit_next (&hit, (caddr_t *) & qck, (caddr_t *) & pqcd))
    {
      int first = 1, inx;
      qcd = *pqcd;
      DO_BOX (query_t *, qr, inx, qcd->qcd_queries)
      {
	if (!qr)
	  continue;
	if (first)
	  {
	    trset_printf ("stmt %s\n", qr->qr_text);
	    first = 0;
	  }
	trset_printf (" %d used %d in use  last use %d ms ago\n", qr->qr_qce->qce_n_used, qr->qr_qce->qce_ref_count,
	    now - qr->qr_qce->qce_last_used);;
      }
      END_DO_BOX;
    }
  mutex_leave (&qrc_mtx);
}


void
qrc_init ()
{
  dk_mutex_init (&qrc_mtx, MUTEX_TYPE_SHORT);
  mutex_option (&qrc_mtx, "qrc", NULL, NULL);
  qr_cache = id_hash_allocate (311, sizeof (qc_key_t), sizeof (qc_data_t), (hash_func_t) qck_hash, (cmp_func_t) qck_cmp);
  qr_cache->ht_rehash_threshold = 80;
  bif_define ("qrc_clear", bif_qrc_clear);
}
