/*  SQL plan subsumption
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
 */




#include "sqlnode.h"
#include "eqlcomp.h"
#include "sqlfn.h"
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "sqlo.h"
#include "rdfinf.h"
#include "strlike.h"



int
sqlo_pred_is_range (ST * pred, ST ** col, ST ** lower, ST ** upper)
{
  switch (pred->type)
    {
    case BOP_LT:;

    }
  return 1;
}



#define BOP_IS_CMP(st) (st->type >= BOP_EQ && st->type <= BOP_GTE)

int
sqlo_is_range (ST * cond, ST * col, dk_set_t * eqs, dk_set_t * lts, dk_set_t * ltes, dk_set_t * gts, dk_set_t * gtes)
{
}


int
sqlo_all_conds (dk_set_t cl, ST * col, dk_set_t * eqs, dk_set_t * lts, dk_set_t * ltes, dk_set_t * gts, dk_set_t * gtes)
{
  /* cl is a a list of or'ed eq or range conditions on col.  If there is a member of cl that is not an eq or range of col, return 0.  If all are such, return 1 and put all the eqs, lts etc in the corresponding list */
  DO_SET (ST *, cond, &cl)
  {
    if (!sqlo_is_range (cond, col, &eqs, &lts, &ltes, &gts, &gtes))
      return 0;
  }
  END_DO_SET ();
  return 1;
}

ST *
sqlo_common_col (dk_set_t cl)
{
  /* see if all conditions are of the same col */
#if 0
  ST *first = (ST *) cl->data;
  if (BOP_AND == first->type)
    {
      dk_set_t cl2 = sqlo_st_connective_list (first, BOP_AND);
      first = (ST *) cl2->data;
    }
  if (!BOP_IS_CMP (first))
    return 0;
  if (ST_P (first->_.bin_exp.left, COL_DOTTED) && sqlo_in_all_cmps (cl))
    ;
#endif
  return NULL;
}


void
sqlo_range_or (ST ** ptree)
{
#if 0
  ST *tree = *ptree;
  if (ST_P (tree, BOP_OR))
    {
      dk_set_t cl = sqlo_st_connective_list (tree, BOP_OR);
      ST *col = sqlo_common_col (cl);
    }
#endif
}





void
dfe_set_eq_set (sqlo_t * so, op_table_t * ot, op_table_t * org_ot, df_elt_t * col, df_elt_t * eq_col, dtp_t eq_set, dk_set_t * res)
{
  ST *eq_st;
  df_elt_t *eq_pred;
  if (dk_set_member (*res, eq_col))
    return;
  BIN_OP (eq_st, BOP_EQ, col->dfe_tree, eq_col->dfe_tree) eq_pred = sqlo_df (so, eq_st);
  eq_pred->_.bin.eq_set = eq_set;
  eq_pred->_.bin.eq_other_dt = ot != org_ot;
  t_set_push (res, eq_pred);
}


int
dfe_col_of_oj (df_elt_t * col)
{
  /* is dfe bound by an optional?  If so, does not go into eq set */
  DO_SET (op_table_t *, tb_ot, &col->dfe_tables)
  {
    if (tb_ot->ot_is_outer)
      return 1;
  }
  END_DO_SET ();
  return 0;
}



void
sqlo_exp_subq_implied_eqs (sqlo_t * so, df_elt_t * dfe)
{
  int inx;
  switch (dfe->dfe_type)
    {
    case SCALAR_SUBQ:
      sqlo_implied_eqs (so, dfe->_.sub.ot);
      break;
    case DFE_BOP:
    case DFE_BOP_PRED:
      if (BOP_NOT == dfe->_.bin.op || BOP_NULL == dfe->_.bin.op)
	return;
      sqlo_exp_subq_implied_eqs (so, dfe->_.bin.left);
      sqlo_exp_subq_implied_eqs (so, dfe->_.bin.right);
    case DFE_CALL:
      DO_BOX (df_elt_t *, par, inx, dfe->_.call.args) sqlo_exp_subq_implied_eqs (so, par);
      END_DO_BOX;
    }
}


void
sqlo_implied_eqs_1 (sqlo_t * so, op_table_t * ot)
{
  sqlo_init_eqs (so, ot, ot->ot_preds);
  DO_SET (df_elt_t *, pred, &ot->ot_preds)
  {
    if (DFE_EXISTS == pred->dfe_type)
      sqlo_implied_eqs (so, pred->_.sub.ot);
    else if (DFE_BOP == pred->dfe_type || DFE_BOP_PRED == pred->dfe_type)
      sqlo_exp_subq_implied_eqs (so, pred);
  }
  END_DO_SET ();
}


void
sqlo_col_trans_eq (sqlo_t * so, op_table_t * ot, ST * col, dk_set_t * res)
{
  dk_set_t *place = (dk_set_t *) id_hash_get (ot->ot_eq_hash, (caddr_t) & col);
  if (place)
    {
      DO_SET (df_elt_t *, eq_col, place)
      {
	if (!dk_set_member (*res, eq_col))
	  {
	    t_set_push (res, (void *) eq_col);
	    sqlo_col_trans_eq (so, ot, eq_col->dfe_tree, res);
	  }
      }
      END_DO_SET ();
    }
}



void
sqlo_trans_eqs (sqlo_t * so, op_table_t * ot)
{
  /* for each col that is eq with another, expand the set to be all transitively eq cols */
  dk_hash_t *cols_done = hash_table_allocate (31);
  DO_IDHASH (ST *, col, dk_set_t, eqs, ot->ot_eq_hash)
  {
    dk_set_t res = NULL;
    if (gethash ((void *) col, cols_done))
      continue;
    sqlo_col_trans_eq (so, ot, col, &res);
    if (res)
      {
	t_set_push (&res, sqlo_df (so, col));
	DO_SET (df_elt_t *, eq_dfe, &res)
	{
	  sethash ((void *) eq_dfe, cols_done, (void *) 1);
	  id_hash_set (ot->ot_eq_hash, (caddr_t) & eq_dfe->dfe_tree, (caddr_t) & res);
	}
	END_DO_SET ();
      }
  }
  END_DO_IDHASH;
  hash_table_free (cols_done);
}



void
sqlo_find_eqs (sqlo_t * so, op_table_t * ot, pred_score_t * ps, join_plan_t * jp)
{
  df_elt_t *col = ps->ps_right;
  df_elt_t *left = col == ps->ps_pred->_.bin.right ? ps->ps_pred->_.bin.left : ps->ps_pred->_.bin.right;
  ST *new_eq;
  id_hash_t *ht = so->so_this_dt->ot_eq_hash;
  df_elt_t *new_pred;
  dk_set_t *place = ht ? (dk_set_t *) id_hash_get (ht, (caddr_t) & col->dfe_tree) : NULL;
  int ck;
  op_table_t *new_ot;
  if (place)
    {
      DO_SET (df_elt_t *, eq_col, place)
      {
	if (eq_col == col || eq_col == left)
	  continue;
	BIN_OP (new_eq, BOP_EQ, left->dfe_tree, eq_col->dfe_tree);
	new_pred = sqlo_df (so, new_eq);
	if (jp->jp_n_preds >= sizeof (jp->jp_preds) / sizeof (pred_score_t))
	  break;
	for (ck = 0; ck < jp->jp_n_preds; ck++)
	  {
	    df_elt_t *prev = jp->jp_preds[ck].ps_pred;
	    if (new_pred == prev
		|| (dfe_is_eq_pred (prev) && new_pred->_.bin.right == prev->_.bin.left
		    && new_pred->_.bin.left == prev->_.bin.right))
	      goto next;
	  }
	ps = &jp->jp_preds[jp->jp_n_preds++];
	memzero (ps, sizeof (pred_score_t));
	ps->ps_pred = new_pred;
	ps->ps_right = new_pred->_.bin.right;
	jp_ps_init_card (jp, ps);
	if (DFE_COLUMN == eq_col->dfe_type && eq_col->dfe_tables)
	  {
	    new_ot = (op_table_t *) eq_col->dfe_tables->data;
	    if (jp->jp_n_joined < JP_MAX_PREDS)
	      jp->jp_joined[jp->jp_n_joined++] = new_ot->ot_dfe;
	  }
      next:;
      }
      END_DO_SET ();
    }
}


void
jp_expand_eq_set (sqlo_t * so, join_plan_t * jp)
{
  int inx, n_preds = jp->jp_n_preds;
  for (inx = 0; inx < n_preds; inx++)
    {
      pred_score_t *ps = &jp->jp_preds[inx];
      if (dfe_is_eq_pred (ps->ps_pred) && ps->ps_right && DFE_COLUMN == ps->ps_right->dfe_type)
	{
	  sqlo_find_eqs (so, so->so_this_dt, ps, jp);
	}
    }
}






/* compare plan fragments */


int
st_cmp_n (sql_comp_t * sc, ST * st1, ST * st2)
{
  int inx;
  if (st1 == st2)
    return 1;
  if (DV_ARRAY_OF_POINTER == DV_TYPE_OF (st1) && DV_ARRAY_OF_POINTER == DV_TYPE_OF (st2) && st1->type == st2->type)
    {
      switch (st1->type)
	{
	case COL_DOTTED:
	  {
	    if (box_equal (st1->_.col_ref.name, st2->_.col_ref.name))
	      {
		void *cn1 = gethash ((void *) (ptrlong) OT_NO (st1->_.col_ref.prefix), sc->sc_cn_normalize);
		void *cn2 = gethash ((void *) (ptrlong) (CN_NN_STEP + OT_NO (st2->_.col_ref.prefix)), sc->sc_cn_normalize);
		return cn1 == cn2;
	      }
	    return 0;
	  }
	default:
	  if (BOX_ELEMENTS (st1) != BOX_ELEMENTS (st2))
	    return 0;
	  DO_BOX (ST *, sub, inx, st1)
	  {
	    if (!st_cmp_n (sc, sub, ((ST **) st2)[inx]))
	      return 0;
	  }
	  END_DO_BOX;
	  return 1;
	}
    }
  if (box_equal (st1, st2))
    return 1;
  return 0;
}


void
dfe_cn_normalize (sql_comp_t * sc, op_table_t * ot1, op_table_t * ot2, dfe_reuse_t * dfr)
{
  int cn1 = OT_NO (ot1->ot_new_prefix);
  int cn2 = OT_NO (ot2->ot_new_prefix);
  int norm = ++sc->sc_cn_ctr1;
  if (!sc->sc_cn_normalize)
    sc->sc_cn_normalize = hash_table_allocate (11);
  if (!dfr->dfr_cn_map)
    dfr->dfr_cn_map = hash_table_allocate (11);
  sethash ((void *) (ptrlong) cn1, sc->sc_cn_normalize, (void *) (ptrlong) norm);
  sethash ((void *) (ptrlong) (cn2 + CN_NN_STEP), sc->sc_cn_normalize, (void *) (ptrlong) norm);
  sethash ((void *) (ptrlong) cn2, dfr->dfr_cn_map, (void *) (ptrlong) cn1);
}


df_elt_t *
tb_has_pred (sql_comp_t * sc, df_elt_t * tb, df_elt_t * pred)
{
  DO_SET (df_elt_t *, pred2, &tb->_.table.all_preds)
  {
    if (st_cmp_n (sc, pred->dfe_tree, pred2->dfe_tree))
      return pred2;
  }
  END_DO_SET ();
  return NULL;
}


int
sqlo_dfe_table_reuse (sql_comp_t * sc, df_elt_t * tb1, df_elt_t * tb2, dfe_reuse_t * dfr)
{
  df_elt_t *common2;
  dk_set_t in_common = NULL;
  if (tb1->_.table.key != tb2->_.table.key)
    return 0;
  dfe_cn_normalize (sc, tb1->_.table.ot, tb2->_.table.ot, dfr);
  DO_SET (df_elt_t *, pred1, &tb1->_.table.all_preds)
  {
    if (!(common2 = tb_has_pred (sc, tb2, pred1)))
      return 0;
    t_set_push (&in_common, (void *) common2);
  }
  END_DO_SET ();
  DO_SET (df_elt_t *, pred2, &tb2->_.table.all_preds)
  {
    if (!dk_set_member (in_common, pred2))
      t_set_push (&dfr->dfr_extra_preds, (void *) pred2);
  }
  END_DO_SET ();
  return 1;
}


int
sqlo_dt_reuse (sql_comp_t * sc, df_elt_t * dt1, df_elt_t * dt2, dfe_reuse_t * dfr)
{
  df_elt_t *e1, *e2 = dt2->_.sub.first;
  for (e1 = dt1->_.sub.first; e1 && e2; e1 = e1->dfe_next, e2 = e2->dfe_next)
    {
      if (e1->dfe_type != e2->dfe_type)
	break;
      switch (e1->dfe_type)
	{
	case DFE_TABLE:
	  if (!sqlo_dfe_table_reuse (sc, e1, e2, dfr))
	    goto differ;
	  break;
	default:
	  goto differ;
	}
    }
  return 1;
differ:;
  return 0;
}


int
sqlo_dfe_reuse (sqlo_t * so, df_elt_t * prev, df_elt_t * new_dfe, dfe_reuse_t * dfr)
{
  sql_comp_t *sc = so->so_sc;
  if (prev->dfe_type != new_dfe->dfe_type)
    return 0;
  if (DFE_DT == prev->dfe_type)
    return sqlo_dt_reuse (sc, prev, new_dfe, dfr);
  if (DFE_TABLE == prev->dfe_type)
    return sqlo_dfe_table_reuse (sc, prev, new_dfe, dfr);
  return 0;
}


void
sqlo_implied_eqs (sqlo_t * so, op_table_t * ot)
{
  sqlo_implied_eqs_1 (so, ot);
  sqlo_trans_eqs (so, so->so_top_ot);
}

int enable_hash_fill_reuse = 1;

int
sqlo_hash_fill_reuse (sqlo_t * so, df_elt_t ** fill_dfe, float *fill_unit, dfe_reuse_t ** dfr_ret)
{
  op_table_t *top_ot = so->so_top_ot;
  df_elt_t *new_filler = *fill_dfe;
  if (!enable_hash_fill_reuse)
    return 0;
  DO_SET (df_elt_t *, filler, &top_ot->ot_hash_fillers)
  {
    dfe_reuse_t dfr;
    memzero (&dfr, sizeof (dfr));
    if (sqlo_dfe_reuse (so, filler, new_filler, &dfr) && !dfr.dfr_extra_preds && !dfr.dfr_extra_cols)
      {
	*fill_dfe = filler;
	*fill_unit = 0;
	*dfr_ret = (dfe_reuse_t *) t_alloc (sizeof (dfe_reuse_t));
	memcpy (*dfr_ret, &dfr, sizeof (dfe_reuse_t));
	return 1;
      }
  }
  END_DO_SET ();
  return 0;
}


df_elt_t *
sqlo_dfe_in_reuse (sqlo_t * so, df_elt_t * ref, dfe_reuse_t * dfr)
{
  /* if a dfe is being reused, this translates a dfe of the reusing context into the reused context */
  ST *rep;
  if (!dfr)
    return ref;
  rep = (ST *) t_box_copy_tree ((caddr_t) ref->dfe_tree);
  DO_HT (ptrlong, ref_cn, ptrlong, reuse_cn, dfr->dfr_cn_map)
  {
    char old[10];
    char np[10];
    caddr_t new_pref;
    snprintf (np, sizeof (np), "t%d", (int) reuse_cn);
    snprintf (old, sizeof (old), "t%d", (int) ref_cn);
    new_pref = t_box_string (np);
    sqlo_col_pref_replace (rep, old, new_pref);
  }
  END_DO_HT;
  return sqlo_df (so, rep);
}


df_elt_t *
sqlo_shared_hash_fill_col (sqlo_t * so, df_elt_t * tb_dfe, df_elt_t * dfe)
{
  /* if a hash build is shared and a col must be added the col must be renamed to the cname of teh actual build */
  char pref2[10];
  ST *tree;
  dfe_reuse_t *dfr;
  ptrlong cn, int_cn;
  df_elt_t *fill_dfe;
  if (DFE_TABLE == tb_dfe->dfe_type)
    {
      if (!tb_dfe->_.table.reuses_hash_filler)
	return dfe;
      fill_dfe = tb_dfe->_.table.hash_filler;
      dfr = tb_dfe->_.table.hash_filler_reuse;
    }
  else
    {
      if (!tb_dfe->_.sub.reuses_hash_filler)
	return dfe;
      fill_dfe = tb_dfe->_.sub.hash_filler;
      dfr = tb_dfe->_.sub.hash_filler_reuse;
    }
  cn = OT_NO (dfe->dfe_tree->_.col_ref.prefix);
  if (!dfr->dfr_cn_map)
    return dfe;
  int_cn = (ptrlong) gethash ((void *) cn, dfr->dfr_cn_map);
  snprintf (pref2, sizeof (pref2), "t%d", (int) int_cn);
  tree = t_listst (3, COL_DOTTED, t_box_string (pref2), dfe->dfe_tree->_.col_ref.name);
  return sqlo_df (so, tree);
}


void
dfr_done (sqlo_t * so, dfe_reuse_t * dfr)
{
  if (!dfr)
    return;
  t_set_push (&so->so_sc->sc_dfe_reuses, (void *) dfr);
}


void
sc_dfr_free (sql_comp_t * sc)
{
  DO_SET (dfe_reuse_t *, dfr, &sc->sc_dfe_reuses)
  {
    if (dfr->dfr_cn_map)
      {
	hash_table_free (dfr->dfr_cn_map);
	dfr->dfr_cn_map = NULL;
      }
  }
  END_DO_SET ();
}
