/*
 *  sqljp.c
 *
 *  $Id$
 *
 *  Join plan - Reduce SQL opt search space
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
#include "eqlcomp.h"
#include "sqlfn.h"
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "sqlo.h"
#include "rdfinf.h"
#include "strlike.h"

dk_set_t dfe_tables (df_elt_t * dfe);

df_elt_t *
dfe_left_col (df_elt_t * tb_dfe, df_elt_t * pred)
{
  df_elt_t **in_list;
  if (DFE_BOP != pred->dfe_type && DFE_BOP_PRED != pred->dfe_type)
    return NULL;
  in_list = sqlo_in_list (pred, tb_dfe, NULL);
  if (in_list && dfe_tables (pred) && !pred->dfe_tables->next)
    return in_list[0];
  if (DFE_COLUMN == pred->_.bin.left->dfe_type && tb_dfe->_.table.ot == (op_table_t *) pred->_.bin.left->dfe_tables->data)
    return pred->_.bin.left;
  if (DFE_COLUMN == pred->_.bin.right->dfe_type && tb_dfe->_.table.ot == (op_table_t *) pred->_.bin.right->dfe_tables->data)
    return pred->_.bin.right;
  return NULL;
}

df_elt_t *
dfe_right (df_elt_t * tb_dfe, df_elt_t * pred)
{
  df_elt_t **in_list;
  if (DFE_BOP != pred->dfe_type && DFE_BOP_PRED != pred->dfe_type)
    return NULL;
  in_list = sqlo_in_list (pred, tb_dfe, NULL);
  if (in_list && dfe_tables (pred) && !pred->dfe_tables->next)
    {
      if (BOX_ELEMENTS (in_list) > 1)
	return in_list[1];
      return NULL;
    }
  if (!dk_set_member (pred->_.bin.left->dfe_tables, (void *) tb_dfe->_.table.ot))
    return pred->_.bin.left;
  if (!dk_set_member (pred->_.bin.right->dfe_tables, (void *) tb_dfe->_.table.ot))
    return pred->_.bin.right;
  return NULL;
}


int
dfe_is_quad (df_elt_t * tb_dfe)
{
  return (DFE_TABLE == tb_dfe->dfe_type && tb_is_rdf_quad (tb_dfe->_.table.ot->ot_table));
}


caddr_t dv_iri_short_name (caddr_t x);

char *
dfe_p_const_abbrev (df_elt_t * tb_dfe)
{
  static char tmp[20];
  caddr_t name;
  int len;
  DO_SET (df_elt_t *, pred, &tb_dfe->_.table.col_preds)
  {
    if (PRED_IS_EQ (pred) && pred->_.bin.left->dfe_type == DFE_COLUMN && 'P' == toupper (pred->_.bin.left->_.col.col->col_name[0]))
      {
	df_elt_t *right = pred->_.bin.right;
	if (DFE_CONST == right->dfe_type && DV_IRI_ID == DV_TYPE_OF (right->dfe_tree))
	  {
	    caddr_t box = dv_iri_short_name ((caddr_t) right->dfe_tree);
	    strncpy (tmp, box ? box : "unnamed", sizeof (tmp));
	    tmp[sizeof (tmp) - 1] = 0;
	    dk_free_box (box);
	  }
	else if ((name = sqlo_iri_constant_name (pred->_.bin.right->dfe_tree)))
	  {
	    caddr_t pref, local;
	    if (iri_split (name, &pref, &local))
	      {
		dk_free_box (pref);
		len = box_length (local) - 5;
		strncpy (tmp, local + 4 + (len > sizeof (tmp) ? len - sizeof (tmp) : 0), sizeof (tmp));
		tmp[sizeof (tmp) - 1] = 0;
		dk_free_box (local);
		return tmp;
	      }
	  }
	else
	  tmp[0] = 0;
	return tmp;
      }
  }
  END_DO_SET ();
  return "";
}


float *
dfe_p_stat (df_elt_t * tb_dfe, df_elt_t * pred, iri_id_t pid, dk_set_t * parts_ret, dbe_column_t * o_col, float **o_stat_ret)
{
  int tried = 0;
#if 0
  caddr_t ctx_name = sqlo_opt_value (tb_dfe->_.table.ot->ot_opts, OPT_RDF_INFERENCE);
  rdf_inf_ctx_t **place = ctx_name ? (rdf_inf_ctx_t **) id_hash_get (rdf_name_to_ric, (caddr_t) & ctx_name) : NULL;
#endif
  dbe_key_t *pk = tb_dfe->_.table.ot->ot_table->tb_primary_key;
  dbe_key_t *o_key = NULL;
  float *p_stat;
again:
  if (!pk->key_p_stat)
    {
      if (tried)
	return NULL;
      tried = 1;
      dfe_init_p_stat (tb_dfe, pred);
      goto again;
    }
  if (o_col)
    o_key = tb_px_key (tb_dfe->_.table.ot->ot_table, o_col);
  p_stat = (float *) id_hash_get (pk->key_p_stat, (caddr_t) & pid);
  *parts_ret = pk->key_parts;
  if (o_key && o_stat_ret && o_key->key_p_stat)
    *o_stat_ret = (float *) id_hash_get (o_key->key_p_stat, (caddr_t) & pid);
  else if (o_stat_ret)
    o_stat_ret = NULL;
  if (!p_stat || (o_stat_ret && !o_stat_ret))
    {
      if (tried)
	return p_stat;
      tried = 1;
      dfe_init_p_stat (tb_dfe, pred);
      goto again;
    }
  return p_stat;
}

#define iri_id_check(x) (DV_IRI_ID == DV_TYPE_OF (x) ? unbox_iri_id (x) : 0)
dbe_key_t *
rdf_po_key (df_elt_t * tb_dfe)
{
  DO_SET (dbe_key_t *, key, &tb_dfe->_.table.ot->ot_table->tb_keys)
  {
    if (!key->key_no_pk_ref && key->key_parts->next && key->key_parts->next->next
	&& toupper (((dbe_column_t *) key->key_parts->data)->col_name[0]) == 'P'
	&& toupper (((dbe_column_t *) key->key_parts->next->data)->col_name[0]) == 'O')
      return key;
  }
  END_DO_SET ();
  return NULL;
}


float
sqlo_rdfs_type_card (df_elt_t * tb_dfe, df_elt_t * p_dfe, df_elt_t * o_dfe)
{
  df_elt_t *lower[2];
  df_elt_t *upper[2];
  index_choice_t ic;
  locus_t *loc_save = tb_dfe->dfe_locus;
  float ret;
  memzero (&ic, sizeof (ic));
  lower[0] = p_dfe;
  lower[1] = o_dfe;
  upper[0] = upper[1] = NULL;
  ic.ic_key = rdf_po_key (tb_dfe);
  if (!ic.ic_key)
    return dbe_key_count (tb_dfe->_.table.ot->ot_table->tb_primary_key);
  ic.ic_ric = rdf_name_to_ctx (sqlo_opt_value (tb_dfe->_.table.ot->ot_opts, OPT_RDF_INFERENCE));
  tb_dfe->dfe_locus = LOC_LOCAL;
  ret = sqlo_inx_sample (tb_dfe, ic.ic_key, lower, upper, 2, &ic);
  tb_dfe->dfe_locus = loc_save;
  return ret;
}


int
dfe_is_iri_id_test (df_elt_t * pred)
{
  df_elt_t *rhs;
  if (DFE_BOP != pred->dfe_type || BOP_NOT != pred->_.bin.op)
    return 0;
  pred = pred->_.bin.left;
  if (DFE_BOP_PRED != pred->dfe_type || BOP_EQ != pred->_.bin.op || 0 != unbox ((ccaddr_t) pred->_.bin.left->dfe_tree))
    return 0;
  rhs = pred->_.bin.right;
  if (st_is_call (rhs->dfe_tree, "isiri_id", 1)
      || (DFE_BOP == rhs->dfe_type && rhs->_.bin.right && st_is_call (rhs->_.bin.right->dfe_tree, "isiri_id", 1)))
    return 1;
  return 0;
}


extern caddr_t rdfs_type;



pred_score_t *
jp_find_ps (join_plan_t * jp, dbe_column_t * col, int is_right)
{
  int inx;
  for (inx = 0; inx < jp->jp_n_preds; inx++)
    {
      pred_score_t *ps = &jp->jp_preds[inx];
      if (!ps->ps_is_placeable)
	continue;
      if (!is_right && ps->ps_left_col == col)
	return ps;
      if (is_right && ps->ps_right && DFE_COLUMN == ps->ps_right->dfe_type && col == ps->ps_right->_.col.col)
	return ps;
    }
  return NULL;
}

void
jp_pk_fanout (join_plan_t * jp)
{
  dbe_key_t *pk = jp->jp_tb_dfe->_.table.ot->ot_table->tb_primary_key;
  int n_parts = pk->key_n_significant;
  int nth = 0;
  float start_card, card;
  start_card = dfe_scan_card (jp->jp_tb_dfe);
  card = start_card;
  DO_SET (dbe_column_t *, part, &pk->key_parts)
  {
    pred_score_t *ps = jp_find_ps (jp, part, 0);
    if (!ps || !ps->ps_pred || !dfe_is_eq_pred (ps->ps_pred))
      return;
    nth++;
    if (nth == n_parts)
      {
	ps->ps_card = 1 / card;
	jp->jp_unique = 1;
	break;
      }
    card *= ps->ps_card;
  }
  END_DO_SET ();
}


int
jp_already_seen (pred_score_t * ps, oid_t * cols, int *col_fill)
{
  int n_cols = *col_fill, inx;
  if (!dfe_is_eq_pred (ps->ps_pred) || !ps->ps_left_col)
    return 0;
  for (inx = 0; inx < n_cols; inx++)
    if (cols[inx] == ps->ps_left_col->col_id)
      return 1;
  if (n_cols >= 10)
    return 1;
  cols[*col_fill] = ps->ps_left_col->col_id;
  (*col_fill)++;
  return 0;
}


float
jp_fanout (join_plan_t * jp)
{
  /* for sql this is the table card over the col pred cards , for rdf this is based on p stat */
  int col_fill = 0;
  oid_t cols[10];
  dbe_column_t *o_col = NULL;
  int jinx;
  if (dfe_is_quad (jp->jp_tb_dfe))
    {
      dk_set_t parts = NULL;
      int nth_col = 0;
      float *p_stat, *o_stat = NULL;
      float s_card, o_card, g_card, misc_card = 1;
      iri_id_t p = 0, s = 0, g = 0;
      caddr_t o = NULL;
      df_elt_t *is_p = NULL, *is_s = NULL, *is_o = NULL, *is_g = NULL;
      for (jinx = 0; jinx < jp->jp_n_preds; jinx++)
	{
	  pred_score_t *ps = &jp->jp_preds[jinx];
	  if (!ps->ps_is_placeable)
	    continue;
	  if (!ps->ps_left_col)
	    {
	      if (dfe_is_iri_id_test (ps->ps_pred))
		continue;
	      misc_card *= 0.3;
	      continue;
	    }
	  if (!PRED_IS_EQ (ps->ps_pred))
	    {
	      misc_card *= 0.5;
	      continue;
	    }
	  switch (ps->ps_left_col->col_name[0])
	    {
	    case 'P':
	    case 'p':
	      is_p = ps->ps_pred;
	      p = iri_id_check (ps->ps_const);
	      break;
	    case 'S':
	    case 's':
	      is_s = ps->ps_pred;
	      s = iri_id_check (ps->ps_const);
	      break;
	    case 'O':
	    case 'o':
	      o_col = ps->ps_left_col;
	      is_o = ps->ps_pred;
	      o = ps->ps_const;
	      break;
	    case 'G':
	    case 'g':
	      is_g = ps->ps_pred;
	      g = iri_id_check (ps->ps_const);
	      break;
	    }
	}
      if (!is_p || !p)
	goto general;
      if (is_o && is_s && is_g)
	jp->jp_unique = 1;
      if (unbox_iri_id (rdfs_type) == p)
	{
	  if (is_s && is_o)
	    return jp->jp_fanout = 0.9;
	  if (is_o && o)
	    return jp->jp_fanout = sqlo_rdfs_type_card (jp->jp_tb_dfe, is_p, is_o);
	}
      p_stat = dfe_p_stat (jp->jp_tb_dfe, is_p, p, &parts, o_col, &o_stat);
      if (!p_stat)
	goto general;

      DO_SET (dbe_column_t *, part, &parts)
      {
	switch (part->col_name[0])
	  {
	  case 'S':
	  case 's':
	    s_card = p_stat[nth_col];
	    break;
	  case 'O':
	  case 'o':
	    {
	      if (1 == nth_col)
		{
		  o_card = p_stat[nth_col];
		  break;
		}
	      o_card = o_stat ? o_stat[1] : p_stat[nth_col];
	      break;
	    }
	  case 'G':
	  case 'g':
	    g_card = p_stat[nth_col];
	    break;
	  }
	nth_col++;
      }
      END_DO_SET ();
      if (is_s && !is_o)
	return jp->jp_fanout = (p_stat[0] / s_card) * misc_card;
      if (!is_s && is_o)
	return jp->jp_fanout = (p_stat[0] / o_card) * misc_card;
      if (is_s && is_o)
	return (p_stat[0] / s_card / o_card) * misc_card;
      return jp->jp_fanout = p_stat[0];
    }
  else if (DFE_TABLE == jp->jp_tb_dfe->dfe_type)
    {
      float card;
    general:
      col_fill = 0;
      jp_pk_fanout (jp);
      card = dbe_key_count (jp->jp_tb_dfe->_.table.ot->ot_table->tb_primary_key);
      for (jinx = 0; jinx < jp->jp_n_preds; jinx++)
	{
	  pred_score_t *ps = &jp->jp_preds[jinx];
	  if (ps->ps_is_placeable)
	    {
	      if (!jp_already_seen (ps, cols, &col_fill))
		card *= ps->ps_card;
	    }
	}
      jp->jp_fanout = card;
    }
  else
    jp->jp_fanout = 0.9;
  return jp->jp_fanout;
}


int
dfe_const_value (df_elt_t * dfe, caddr_t * data_ret)
{
  caddr_t name;
  if (DFE_CONST == dfe->dfe_type)
    {
      *data_ret = (caddr_t) dfe->dfe_tree;
      return 1;
    }
  if ((name = sqlo_iri_constant_name (dfe->dfe_tree)))
    {
      caddr_t id = key_name_to_iri_id (NULL, name, 0);
      *data_ret = id;
      if (!*data_ret)
	return 0;
      *data_ret = t_full_box_copy_tree (id);
      dk_free_tree (id);
      return 1;
    }
  if ((name = sqlo_rdf_obj_const_value (dfe->dfe_tree, NULL, NULL)))
    {
      *data_ret = name;
      return 1;
    }
  return 0;
}


#define JPF_TRY 4
#define JPF_HASH 8
#define JPF_NO_PLACED_JOINS 16


int enable_jp_red_pred = 1;

int
pred_is_same (op_table_t * ot, df_elt_t * eq1, df_elt_t * eq2)
{
  if (sqlo_is_col_eq (ot, eq1->_.bin.left, eq2->_.bin.left) && sqlo_is_col_eq (ot, eq1->_.bin.right, eq2->_.bin.right))
    return 1;
  if (sqlo_is_col_eq (ot, eq1->_.bin.left, eq2->_.bin.right) && sqlo_is_col_eq (ot, eq1->_.bin.right, eq2->_.bin.left))
    return 1;
  return 0;
}


int
dfe_pred_is_redundant (df_elt_t * first_tb, df_elt_t * pred)
{
  /* true if an identical pred occurs in the col preds of the first_tb */
  op_table_t *ot = first_tb->_.table.ot->ot_super;
  if (!ot)
    return 0;
  if (!enable_jp_red_pred)
    return 0;
  if (!dfe_is_eq_pred (pred) || DFE_COLUMN != pred->_.bin.left->dfe_type || DFE_COLUMN != pred->_.bin.right->dfe_type)
    return 0;
  DO_SET (df_elt_t *, first_pred, &first_tb->_.table.col_preds)
  {
    if (dfe_is_eq_pred (first_pred)
	&& DFE_COLUMN == first_pred->_.bin.left->dfe_type && DFE_COLUMN == first_pred->_.bin.right->dfe_type
	&& pred_is_same (ot, first_pred, pred))
      return 1;
  }
  END_DO_SET ();
  return 0;
}


df_elt_t *
dfe_extract_or (sqlo_t * so, df_elt_t * tb_dfe, df_elt_t * pred)
{
  dk_set_t list;
  ST *res = NULL;
  if (DFE_BOP != pred->dfe_type || BOP_OR != pred->_.bin.op)
    return NULL;
  list = sqlo_connective_list (pred, BOP_OR);
  DO_SET (df_elt_t *, term, &list)
  {
    ST *and_res = NULL;
    dk_set_t and_list;
    int any_added = 0;
    if (!ST_P (term->dfe_tree, BOP_AND))
      return NULL;
    if (!dk_set_member (term->dfe_tables, (void *) tb_dfe->_.table.ot))
      return NULL;
    and_list = sqlo_connective_list (term, BOP_AND);
    DO_SET (df_elt_t *, and_term, &and_list)
    {
      if (and_term->dfe_tables && tb_dfe->_.table.ot == (op_table_t *) and_term->dfe_tables->data && !and_term->dfe_tables->next)
	{
	  t_st_and (&and_res, and_term->dfe_tree);
	  any_added = 1;
	}
    }
    END_DO_SET ();
    if (!any_added)
      return NULL;
    t_st_or (&res, and_res);

  }
  END_DO_SET ();
  return sqlo_df (so, res);
}

int dfe_ot_is_subset (dk_set_t super_dfes, dk_set_t sub_ots);


int
jp_reqd_placed (join_plan_t * jp, df_elt_t * dfe, df_elt_t * placing)
{
  join_plan_t *top_jp = jp->jp_top_jp;
  if (top_jp->jp_probe_ots)
    {
      /* check whether pred placeable in context of hash build side */
      if (0 && dk_set_intersects (dfe->dfe_tables, top_jp->jp_probe_ots))
	return 0;
      DO_SET (op_table_t *, ot, &dfe->dfe_tables)
      {
	if (placing && ot == placing->_.table.ot)
	  continue;
	if (dk_set_member (top_jp->jp_path, (void *) ot->ot_jp))
	  continue;
	if (!dk_set_member (top_jp->jp_hash_fill_dfes, (void *) ot->ot_dfe))
	  return 0;
      }
      END_DO_SET ();
      return 1;
    }
  DO_SET (op_table_t *, req, &dfe->dfe_tables)
  {
    if (!req->ot_dfe || (!req->ot_dfe->dfe_is_placed && !dk_set_member (top_jp->jp_hash_fill_dfes, req->ot_dfe)))
      return 0;
  }
  END_DO_SET ();
  return 1;
}


int
dfe_ot_is_subset (dk_set_t super_dfes, dk_set_t sub_ots)
{
  DO_SET (op_table_t *, ot, &sub_ots) if (!dk_set_member (super_dfes, (void *) ot))
    return 0;
  END_DO_SET ();
  return 1;
}


int
dk_set_intersects (dk_set_t s1, dk_set_t s2)
{
  DO_SET (void *, elt, &s1) if (dk_set_member (s2, elt))
     return 1;
  END_DO_SET ();
  return 0;
}


void
jp_ps_init_card (join_plan_t * jp, pred_score_t * ps)
{
  if (!ps->ps_left_col)
    {
      df_elt_t *left = dfe_left_col (jp->jp_tb_dfe, ps->ps_pred);
      ps->ps_left_col = left && DFE_COLUMN == left->dfe_type ? left->_.col.col : NULL;
    }
  if (!ps->ps_left_col)
    {
      ps->ps_card = 1;
      return;
    }
  if (-1 != ps->ps_left_col->col_n_distinct)
    {
      ps->ps_card = 1.0 / ps->ps_left_col->col_n_distinct;
    }
  else
    ps->ps_card = 0.5;
}


void
jp_add (join_plan_t * jp, df_elt_t * tb_dfe, df_elt_t * pred, int is_join)
{
  join_plan_t *top_jp = jp->jp_top_jp;
  int n_preds = jp->jp_n_preds;
  caddr_t data;
  uint32 sq_hash = sqlo_subq_id_hash (pred->dfe_tree);
  pred_score_t *ps;
  df_elt_t *right, *left_col;
  if (dk_set_member (tb_dfe->dfe_sqlo->so_inside_subq, (void *) (ptrlong) sq_hash))
    return;
  if (n_preds + 1 >= JP_MAX_PREDS)
    return;
  memzero (&jp->jp_preds[n_preds], sizeof (pred_score_t));
  jp->jp_n_preds = n_preds + 1;
  ps = &jp->jp_preds[n_preds];
  ps->ps_pred = pred;
  left_col = dfe_left_col (tb_dfe, pred);
  if (left_col)
    {
      ps->ps_left_col = left_col->_.col.col;
    }
  right = dfe_right (tb_dfe, pred);
  if (!is_join)
    ps->ps_is_placeable = 1;
  else
    {
      df_elt_t *or_ext = NULL;
      int placeable_decided = 0;
      if (top_jp->jp_hash_fill_dfes)
	{
	  /* see if can import a summary of an or in the outer query into a hash build */
	  if (dfe_ot_is_subset (top_jp->jp_hash_fill_dfes, pred->dfe_tables))
	    placeable_decided = ps->ps_is_placeable = 1;
	  else
	    {
	      or_ext = dfe_extract_or (jp->jp_tb_dfe->dfe_sqlo, jp->jp_tb_dfe, pred);
	      if (or_ext)
		{
		  ps->ps_pred = or_ext;
		  placeable_decided = ps->ps_is_placeable = 1;
		  pred = or_ext;
		}
	    }
	}
      if (!placeable_decided)
	ps->ps_is_placeable = jp_reqd_placed (jp, pred, tb_dfe);
      if ((JPF_NO_PLACED_JOINS & is_join) && ps->ps_is_placeable && !or_ext)
	{
	  jp->jp_n_preds--;
	  return;
	}
      if (jp->jp_hash_fill_dfes)
	{
	  int is_redundant = 0;
	  df_elt_t *first_tb;
	  join_plan_t *root_jp = jp;
	  while (root_jp->jp_prev)
	    root_jp = root_jp->jp_prev;
	  first_tb = root_jp->jp_tb_dfe;
	  DO_SET (op_table_t *, pred_dep, &pred->dfe_tables)
	  {
	    if (pred_dep->ot_dfe != tb_dfe && pred_dep->ot_dfe->dfe_is_placed
		&& !dk_set_member (jp->jp_hash_fill_dfes, (void *) pred_dep->ot_dfe))
	      {
		if (root_jp->jp_fill_selectivity < 0.5 && dfe_pred_is_redundant (first_tb, pred))
		  is_redundant = 1;
		else
		  jp->jp_not_for_hash_fill = 1;
	      }
	  }
	  END_DO_SET ();
	  if (is_redundant)
	    {
	      t_set_push (&root_jp->jp_extra_preds, (void *) pred);
	      jp->jp_n_preds--;
	    }
	}
    }

  if (!right || !ps->ps_left_col || !PRED_IS_EQ_OR_IN (pred))
    {
      ps->ps_card = DFE_TEXT_PRED == pred->dfe_type ? 0.01 : 0.3;
      return;
    }
  ps->ps_right = right;
  if (DFE_COLUMN == right->dfe_type && PRED_IS_EQ_OR_IN (pred))
    {
      df_elt_t *right_tb = ((op_table_t *) right->dfe_tables->data)->ot_dfe;
      if (!right_tb->dfe_is_placed || jp->jp_top_jp->jp_probe_ots)
	{
	  int jinx;
	  for (jinx = 0; jinx < jp->jp_n_joined; jinx++)
	    if (right_tb == jp->jp_joined[jinx])
	      goto already_in;
	  jp->jp_joined[jp->jp_n_joined++] = right_tb;
	already_in:;
	}
      if (1 || (right_tb->dfe_is_placed
	      || (top_jp->jp_hash_fill_dfes && dk_set_member (top_jp->jp_hash_fill_dfes->next, right_tb))))
	{
	  /* after adding a join, set the ps card to be join sel if the joined table is either placed or placed in would be hash build side join */
	  jp_ps_init_card (jp, ps);
	}
    }
  else if (dfe_const_value (right, &data))
    {
      ps->ps_const = data;
      ps->ps_is_const = 1;
      if (ps->ps_left_col && -1 != ps->ps_left_col->col_n_distinct)
	ps->ps_card = 1.0 / ps->ps_left_col->col_n_distinct;
    }
  if (!ps->ps_card)
    ps->ps_card = 0, 5;
}


int
dfe_in_hash_set (df_elt_t * tb_dfe, int hash_set)
{
  if (!hash_set)
    return 1;
  return unbox (sqlo_opt_value (tb_dfe->_.table.ot->ot_opts, OPT_HASH_SET)) == hash_set;
}

void
dfe_jp_fill (sqlo_t * so, op_table_t * ot, df_elt_t * tb_dfe, join_plan_t * jp, int mode, int hash_set)
{
  op_table_t *pred_ot = ot;
  jp->jp_n_joined = jp->jp_n_preds = 0;
  jp->jp_tb_dfe = tb_dfe;
again:
  DO_SET (df_elt_t *, pred, &pred_ot->ot_preds)
  {
    if (pred->dfe_tables && !pred->dfe_tables->next
	&& tb_dfe->_.table.ot == (op_table_t *) pred->dfe_tables->data && dfe_in_hash_set (tb_dfe, hash_set))
      {
	jp_add (jp, tb_dfe, pred, 0);
      }
    else if (dk_set_member (pred->dfe_tables, (void *) tb_dfe->_.table.ot) && dfe_in_hash_set (tb_dfe, hash_set))
      {
	int n_before = jp->jp_n_preds;
	jp_add (jp, tb_dfe, pred, 1 | mode);
	if (jp->jp_top_jp && jp->jp_top_jp->jp_hash_fill_dfes && jp->jp_n_preds > n_before)
	  jp->jp_join_flags |= jp_may_restrict (jp, &jp->jp_preds[n_before]);
	if (jp->jp_n_preds && jp->jp_preds[jp->jp_n_preds - 1].ps_is_placeable)
	  {
	    if (!jp->jp_prev)
	      tb_dfe->dfe_is_joined = 1;
	  }
      }
  }
  END_DO_SET ();
  if (pred_ot != jp->jp_tb_dfe->_.table.ot->ot_super)
    {
      /* if can be that a restricting join is imported into a subq from the enclosing.  So then do the preds in the subq plus the preds in the dt where the restriction comes from */
      pred_ot = pred_ot->ot_super;
      if (pred_ot)
	goto again;
    }
  jp->jp_fanout = jp_fanout (jp);
  if (jp->jp_fanout < 0 && jp->jp_fanout != -1)
    bing ();
}


int
jp_mark_restr_join (sqlo_t * so, join_plan_t * jp, join_plan_t * root_jp)
{
  join_plan_t *prev;
  float path_fanout = 1;
  for (prev = jp; prev; prev = prev->jp_prev)
    {
      path_fanout *= prev->jp_fanout;
    }
  if (so->so_any_placed && path_fanout > 0.8)
    return 0;
  if (-1 == root_jp->jp_best_cost || jp->jp_cost < root_jp->jp_best_cost)
    {
      root_jp->jp_best_card = path_fanout;
      root_jp->jp_best_cost = jp->jp_cost;
      root_jp->jp_best_jp = NULL;
      for (prev = jp; prev; prev = prev->jp_prev)
	t_set_push (&root_jp->jp_best_jp, (void *) prev->jp_tb_dfe);
    }
  return 1;
}


extern int enable_joins_only;

float
dfe_join_score_jp (sqlo_t * so, op_table_t * ot, df_elt_t * tb_dfe, dk_set_t * res, join_plan_t * prev_jp)
{
  join_plan_t jp;
  float score = 0;
  int level = 0, any_tried = 0;
  join_plan_t *root_jp = NULL;
  float path_fanout = 1, root_fanout = 1;
  if (SO_REFINE_PLAN == so->so_plan_mode && !so->so_any_placed)
    {
      t_set_push (res, (void *) t_cons (tb_dfe, NULL));
      return tb_dfe->_.table.ot->ot_initial_cost;
    }
  jp_clear (&jp);
  jp.jp_hash_fill_dfes = NULL;
  jp.jp_best_jp = NULL;
  jp.jp_reached = 0;
  jp.jp_prev = prev_jp;
  for (prev_jp = prev_jp; prev_jp; prev_jp = prev_jp->jp_prev)
    {
      path_fanout *= prev_jp->jp_fanout;
      root_fanout = prev_jp->jp_fanout;
      root_jp = prev_jp;
      level++;
    }
  if (DFE_DT == tb_dfe->dfe_type && tb_dfe->_.sub.ot->ot_trans && level)
    return 0;			/* if trans dt joined and not first, do not go further, can get a case where neither end of trans is fully placed */
  jp.jp_top_jp = root_jp ? root_jp : &jp;
  tb_dfe->dfe_double_placed = tb_dfe->dfe_is_placed != 0;
  tb_dfe->dfe_is_placed = DFE_JP_PLACED;	/* to fool dfe_reqd_placed */
  dfe_jp_fill (so, ot, tb_dfe, &jp, JPF_TRY, 0);
  if (jp.jp_prev)
    jp.jp_cost = jp.jp_prev->jp_cost + path_fanout * jp.jp_fanout;
  else
    {
      jp.jp_cost = jp.jp_fanout;
      if (so->so_any_placed && enable_joins_only && !tb_dfe->dfe_is_joined)
	{
	  t_set_push (res, (void *) t_cons ((void *) tb_dfe, NULL));
	  tb_dfe->dfe_is_placed = 0;
	  return (1.0 / jp.jp_cost) / (1 + jp.jp_n_joined);
	}
    }
  jp.jp_best_card = jp.jp_best_cost = -1;

  if (jp.jp_fanout * path_fanout / (so->so_any_placed ? 1 : root_fanout) < 0.7)
    goto restricting;
  if (jp.jp_n_joined && (level < 2 || (jp.jp_fanout < 1.1 && level < 4)))
    {
      int jinx;
      if (level > 0)
	root_jp->jp_reached += (float) jp.jp_n_joined / level;
      for (jinx = 0; jinx < jp.jp_n_joined; jinx++)
	{
	  dfe_join_score_jp (so, ot, jp.jp_joined[jinx], res, &jp);
	  any_tried = 1;
	}
    }
restricting:
  if (!any_tried && level > 0)
    {
      jp_mark_restr_join (so, &jp, root_jp);
    }
  tb_dfe->dfe_is_placed = 0;
  if (!jp.jp_prev)
    {
      jp.jp_tb_dfe->dfe_arity = jp.jp_best_card;
      if (jp.jp_best_jp)
	t_set_push (res, (void *) jp.jp_best_jp);
      else
	t_set_push (res, (void *) t_cons (tb_dfe, NULL));
      score = (jp.jp_best_cost != -1 ? jp.jp_best_cost : jp.jp_fanout);
      return score;
    }
  return 0;
}


void
jp_print (join_plan_t * jp)
{
  int inx;
  if (jp->jp_prev)
    {
      jp_print (jp->jp_prev);
      printf ("---\n");
    }
  for (inx = 0; inx < jp->jp_n_preds; inx++)
    {
      pred_score_t *ps = &jp->jp_preds[inx];
      printf ("%s", ps->ps_is_placeable ? "p" : "np");
      sqlo_box_print ((caddr_t) ps->ps_pred->dfe_tree);
    }
}


int
sqlo_hash_filler_unique (sqlo_t * so, df_elt_t * hash_ref_tb, df_elt_t * fill_copy)
{
  /* a join in a hash filler for a unique hash ref may destroy uniqueness if all joins atre not gguaranteed cardinality reducing, i.e. fk to pk.
   * Uniqueness is preserved if the filler for the joined table is not unique in the filler and if everything else is. */
  caddr_t head_prefix = hash_ref_tb->_.table.ot->ot_new_prefix;
  df_elt_t *dfe;
  if (fill_copy->_.sub.generated_dfe)
    return sqlo_hash_filler_unique (so, hash_ref_tb, fill_copy);
  for (dfe = fill_copy->_.sub.first; dfe; dfe = dfe->dfe_next)
    {
      if (DFE_DT == dfe->dfe_type)
	return 0;
      if (DFE_TABLE == dfe->dfe_type)
	{
	  if (!strcmp (dfe->_.table.ot->ot_prefix, head_prefix))
	    continue;
	  if (!dfe->_.table.is_unique)
	    return 0;
	}
    }
  return 1;
}


/* hash join with a join on the build side */

void
jp_add_hash_fill_join (join_plan_t * root_jp, join_plan_t * jp)
{
  int n_pk, pos, inx;
  dbe_key_t *pk;
  if (dk_set_member (root_jp->jp_hash_fill_dfes, (void *) jp->jp_tb_dfe))
    return;
  t_set_push (&root_jp->jp_hash_fill_dfes, (void *) jp->jp_tb_dfe);
  pk = jp->jp_tb_dfe->_.table.ot->ot_table->tb_primary_key;
  n_pk = pk->key_n_significant;
  for (inx = 0; inx < jp->jp_n_preds; inx++)
    {
      if (!jp->jp_preds[inx].ps_is_placeable || !dfe_is_eq_pred (jp->jp_preds[inx].ps_pred))
	continue;
      pos = dk_set_position (pk->key_parts, jp->jp_preds[inx].ps_left_col);
      if (pos >= pk->key_n_significant)
	continue;
      n_pk--;
      if (!n_pk)
	break;
    }
  if (n_pk)
    root_jp->jp_hash_fill_non_unq = 1;	/* hash join build side not guaranteed to keep unique, conatins other than pk to fk joins */
}

void
dfe_hash_fill_score (sqlo_t * so, op_table_t * ot, df_elt_t * tb_dfe, join_plan_t * prev_jp, int hash_set)
{
  /* if finds a restricting join path, adds the dfes on the path to the hash filler dfes */
  join_plan_t jp;
  int level = 0, pinx;
  join_plan_t *root_jp = NULL;
  float path_fanout = 1;
  if (DFE_TABLE != tb_dfe->dfe_type || tb_dfe->_.table.ot->ot_is_outer)
    return;
  jp.jp_prev = prev_jp;
  jp.jp_top_jp = prev_jp->jp_top_jp;
  for (prev_jp = prev_jp; prev_jp; prev_jp = prev_jp->jp_prev)
    {
      path_fanout *= prev_jp->jp_fanout;
      root_jp = prev_jp;
      level++;
    }
  if (dk_set_member (root_jp->jp_hash_fill_dfes, (void *) tb_dfe))
    return;
  if (dk_set_member (root_jp->jp_probe_ots, (void *) tb_dfe->_.table.ot))
    return;
  jp.jp_hash_fill_dfes = jp.jp_prev->jp_hash_fill_dfes;
  if (tb_dfe->dfe_is_placed)
    tb_dfe->dfe_double_placed = 1;
  tb_dfe->dfe_is_placed = DFE_JP_PLACED;	/* to fool dfe_reqd_placed */
  jp.jp_not_for_hash_fill = 0;
  dfe_jp_fill (so, ot, tb_dfe, &jp, JPF_HASH, hash_set);
  if (jp.jp_fanout > 1.3 || jp.jp_not_for_hash_fill)
    {
      if (level > 1 && path_fanout / root_jp->jp_fanout < 0.7)
	{
	  join_plan_t *prev;
	  root_jp->jp_best_card *= path_fanout;
	  for (prev = jp.jp_prev; prev; prev = prev->jp_prev)
	    {
	      jp_add_hash_fill_join (root_jp, prev);
	      for (pinx = 0; pinx < prev->jp_n_preds; pinx++)
		if (prev->jp_preds[pinx].ps_is_placeable)
		  t_set_pushnew (&root_jp->jp_hash_fill_preds, (void *) prev->jp_preds[pinx].ps_pred);
	    }
	}
      tb_dfe->dfe_is_placed = tb_dfe->dfe_double_placed ? DFE_PLACED : 0;
      tb_dfe->dfe_double_placed = 0;
      return;
    }
  if (jp.jp_n_joined && level < 4)
    {
      int jinx;
      s_node_t tn;
      tn.data = (void *) tb_dfe;
      tn.next = jp.jp_prev->jp_hash_fill_dfes;
      jp.jp_hash_fill_dfes = &tn;
      for (jinx = 0; jinx < jp.jp_n_joined; jinx++)
	dfe_hash_fill_score (so, ot, jp.jp_joined[jinx], &jp, hash_set);
      jp.jp_hash_fill_dfes = tn.next;
    }
  else if (root_jp)
    {
      /* can be fanout here is 1 but the hash join to the first of the filler is sel;selective.  If som it is worthwhile including functionally dependent joins in the build side */
      float root_fanout = root_jp->jp_fanout / root_jp->jp_tb_dfe->dfe_arity;
      if (path_fanout * jp.jp_fanout / root_fanout < 0.8)
	{
	  join_plan_t *prev = &jp;
	  root_jp->jp_best_card *= path_fanout * jp.jp_fanout / root_fanout;
	  for (prev = &jp; prev; prev = prev->jp_prev)
	    {
	      jp_add_hash_fill_join (root_jp, prev);
	      for (pinx = 0; pinx < prev->jp_n_preds; pinx++)
		if (prev->jp_preds[pinx].ps_is_placeable)
		  t_set_pushnew (&root_jp->jp_hash_fill_preds, (void *) prev->jp_preds[pinx].ps_pred);
	    }
	}
    }
  tb_dfe->dfe_is_placed = tb_dfe->dfe_double_placed ? DFE_PLACED : 0;
  tb_dfe->dfe_double_placed = 0;
}


int enable_hash_fill_join = 1;
int enable_subq_cache = 1;


ST *
sqlo_pred_tree (df_elt_t * dfe)
{
  /* in existence pred dfes the dfe tree does not have the exists, so put it there, else the dfe tree pf a pred is good */
  ST *copy = (ST *) t_box_copy_tree ((caddr_t) dfe->dfe_tree);
  if (DFE_EXISTS == dfe->dfe_type)
    return (ST *) SUBQ_PRED (EXISTS_PRED, NULL, copy, NULL, NULL);
  return copy;
}



void sqlo_dt_eqs (sqlo_t * so, df_elt_t * dfe);


int
dfe_join_unique (sqlo_t * so, op_table_t * ot, df_elt_t * tb_dfe, dk_set_t * res, join_plan_t * prev_jp)
{
  int jinx;
  join_plan_t jp;
  float score = 0;
  int level = 0, any_tried = 0;
  join_plan_t *root_jp = NULL;
  float path_fanout = 1, root_fanout = 1;
  char prev_placed = tb_dfe->dfe_is_placed;
  jp_clear (&jp);
  jp.jp_prev = prev_jp;
  for (prev_jp = prev_jp; prev_jp; prev_jp = prev_jp->jp_prev)
    {
      path_fanout *= prev_jp->jp_fanout;
      root_fanout = prev_jp->jp_fanout;
      root_jp = prev_jp;
      level++;
    }
  if (DFE_DT == tb_dfe->dfe_type)
    return 0;			/* do not go inside a dt or trans dt */
  jp.jp_top_jp = root_jp ? root_jp : &jp;
  tb_dfe->dfe_is_placed = DFE_JP_PLACED;	/* to fool dfe_reqd_placed */
  dfe_jp_fill (so, ot, tb_dfe, &jp, JPF_TRY, 0);
  if (!jp.jp_unique)
    goto no;
  for (jinx = 0; jinx < jp.jp_n_joined; jinx++)
    {
      if (!dfe_join_unique (so, ot, jp.jp_joined[jinx], res, &jp))
	goto no;
    }
  tb_dfe->dfe_is_placed = prev_placed;
  return 1;
no:
  tb_dfe->dfe_is_placed = prev_placed;
  return 0;
}


void
sqlo_pred_tree_eqs (sqlo_t * so, df_elt_t * pred)
{
  if (!IS_BOX_POINTER (pred))
    return;
  switch (pred->dfe_type)
    {
    case DFE_BOP:
    case DFE_BOP_PRED:
      sqlo_pred_tree_eqs (so, pred->_.bin.left);
      sqlo_pred_tree_eqs (so, pred->_.bin.right);
      break;
    case DFE_EXISTS:
    case DFE_VALUE_SUBQ:
      sqlo_dt_eqs (so, pred);
      break;
    default:
      break;
    }
}


void
sqlo_ot_set_placed (op_table_t * ot, int fl)
{
  DO_SET (df_elt_t *, dfe, &ot->ot_from_dfes) dfe->dfe_is_placed = fl;
  END_DO_SET ();
}


int
dfe_depends_on_exterior (op_table_t * ot, df_elt_t * tb_dfe)
{
  /* true if the tb_dfe is joined to an outside context by predicates in ot preds */
  DO_SET (df_elt_t *, pred, &ot->ot_preds)
  {
    if (dk_set_member (pred->dfe_tables, tb_dfe->_.table.ot) && pred->dfe_tables->next && !pred->dfe_tables->next->next)
      {
	op_table_t *ot1 = (op_table_t *) pred->dfe_tables->data;
	op_table_t *ot2 = (op_table_t *) pred->dfe_tables->next->data;
	if (ot1->ot_super != ot2->ot_super)
	  return 1;
      }
  }
  END_DO_SET ();
  return 0;
}


int
sqlo_unq_exists (sqlo_t * so, df_elt_t * dt_dfe)
{
  /* if the dt dfe has top level and existence preds that are in fact unique, pop the top existence into the top level join */
  dk_set_t r = NULL;
  op_table_t *super_ot = dt_dfe->_.sub.ot;
  DO_SET (df_elt_t *, dfe, &super_ot->ot_preds)
  {
    if (DFE_EXISTS != dfe->dfe_type)
      continue;
    op_table_t *ot = dfe->_.sub.ot;
    if (sqlo_opt_value (ot->ot_opts, OPT_NO_EXISTS_JOIN))
      continue;
    DO_SET (df_elt_t *, dfe, &ot->ot_from_dfes)
    {
      if ((DFE_TABLE == dfe->dfe_type || DFE_DT == dfe->dfe_type) && sqlo_opt_value (dfe_ot (dfe)->ot_opts, OPT_NO_EXISTS_JOIN))
	goto not_this;
    }
    END_DO_SET ();
    sqlo_ot_set_placed (ot->ot_super, 1);
    DO_SET (df_elt_t *, dfe, &ot->ot_from_dfes)
    {
      if (DFE_TABLE != dfe->dfe_type)
	continue;
      if (dfe_depends_on_exterior (ot, dfe))
	{
	  if (dfe_join_unique (so, ot, dfe, &r, NULL))
	    goto unq;
	}
    }
    END_DO_SET ();
    sqlo_ot_set_placed (ot->ot_super, 0);
    continue;
  unq:
    sqlo_ot_set_placed (ot->ot_super, 0);
    super_ot->ot_from_ots = dk_set_conc (super_ot->ot_from_ots, ot->ot_from_ots);
    super_ot->ot_from_dfes = dk_set_conc (super_ot->ot_from_dfes, ot->ot_from_dfes);
    DO_SET (op_table_t *, sub, &ot->ot_from_ots)
    {
      sub->ot_super = super_ot;
      sub->ot_dfe->dfe_super = super_ot->ot_dfe;
    }
    END_DO_SET ();
    super_ot->ot_preds = dk_set_conc (super_ot->ot_preds, ot->ot_preds);
    t_set_delete (&super_ot->ot_preds, (void *) dfe);
    return 1;
  not_this:;
  }
  END_DO_SET ();
  return 0;
}

void
sqlo_dt_eqs (sqlo_t * so, df_elt_t * dfe)
{
  op_table_t *ot = dfe->_.sub.ot;
  op_table_t *s_ot;
  while (sqlo_unq_exists (so, dfe));
  sqlo_init_eqs (so, ot, NULL);
  for (s_ot = ot->ot_super; s_ot; s_ot = s_ot->ot_super)
    sqlo_init_eqs (so, dfe->_.sub.ot, s_ot->ot_preds);
  DO_SET (df_elt_t *, pred, &ot->ot_preds)
  {
    sqlo_pred_tree_eqs (so, pred);
  }
  END_DO_SET ();
}

int enable_sqlo_eqs = 1;

void
sqlo_eqs (sqlo_t * so, op_table_t * top_ot)
{
  if (!enable_sqlo_eqs)
    return;
  sqlo_dt_eqs (so, top_ot->ot_dfe);
}


dk_set_t
sqlo_jp_exists_preds (sqlo_t * so, join_plan_t * top_jp, dk_set_t path)
{
  dk_set_t exists_preds = NULL;
  DO_SET (join_plan_t *, jp, &path)
  {
    int inx;
    for (inx = 0; inx < jp->jp_n_preds; inx++)
      {
	df_elt_t *pred = jp->jp_preds[inx].ps_pred;
	if (dfe_is_eq_pred (pred))
	  {
	    DO_SET (df_elt_t *, previous, &exists_preds)
	    {
	      if (dfe_is_eq_pred (previous)
		  && ((pred->_.bin.left == previous->_.bin.right && pred->_.bin.right == previous->_.bin.left)
		      || (pred->_.bin.left == previous->_.bin.left && pred->_.bin.right == previous->_.bin.right)))
		goto already_in;
	    }
	    END_DO_SET ();
	  }
	if (jp_reqd_placed (top_jp, pred, NULL))
	  t_set_pushnew (&exists_preds, (void *) pred);
      already_in:;
      }
  }
  END_DO_SET ();
  return exists_preds;
}


ST *
sqlo_restr_as_exists (sqlo_t * so, dk_set_t path)
{
  /* given one or more consecutive joins, make a sql tree with existence */
  int inx;
  join_plan_t *top_jp = ((join_plan_t *) path->data)->jp_top_jp;
  ST **from = (ST **) t_list_to_array (path);
  ST *pred_tree = NULL;
  ST *texp = t_listst (9, TABLE_EXP, from,
      NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ST *sel = t_listst (5, SELECT_STMT, NULL, t_listst (1, 1), NULL, texp);
  dk_set_t preds = sqlo_jp_exists_preds (so, top_jp, path);
  DO_BOX (join_plan_t *, jp, inx, from)
  {
    df_elt_t *tb_dfe = jp->jp_tb_dfe;
    caddr_t opts = list (2, OPT_JOIN_RESTR, jp->jp_join_flags);
    from[inx] =
	t_listst (3, TABLE_REF, t_listst (6, TABLE_DOTTED, tb_dfe->_.table.ot->ot_table->tb_name, tb_dfe->_.table.ot->ot_new_prefix,
	    NULL, NULL, opts), NULL);
  }
  END_DO_BOX;
  DO_SET (df_elt_t *, dfe, &preds) t_st_and (&pred_tree, sqlo_pred_tree (dfe));
  END_DO_SET ();
  texp->_.table_exp.where = pred_tree;
  return SUBQ_PRED (EXISTS_PRED, NULL, sel, NULL, NULL);
}


int
jp_may_restrict (join_plan_t * jp, pred_score_t * ps)
{
  /* given a predicate, see if the predicate  can be used to join to a table for restricting a hash build side */
  sqlo_t *so = jp->jp_tb_dfe->dfe_sqlo;
  df_elt_t *left;
  join_plan_t *top_jp = jp->jp_top_jp;
  int rc = 0;
  op_table_t *ot, *left_ot;
  df_elt_t *right = ps->ps_right;
  if (sqlo_in_list (ps->ps_pred, NULL, NULL))
    return JP_NO_JOIN;
  if (!right || DFE_COLUMN != right->dfe_type)
    return JP_NO_JOIN;
  ot = (op_table_t *) right->dfe_tables->data;
  if (!ot->ot_table)
    return JP_NO_JOIN;
  if (ot->ot_is_outer)
    return JP_NO_JOIN;
  if ((JP_NO_REIMPORT & (ptrlong) sqlo_opt_value (ot->ot_opts, OPT_JOIN_RESTR)))
    return JP_NO_JOIN;
  if (!ps->ps_left_col)
    return JP_NO_JOIN;
  left = dfe_left_col (jp->jp_tb_dfe, ps->ps_pred);
  left_ot = (op_table_t *) left->dfe_tables->data;
  if (dk_set_member (so->so_hash_probes, (void *) left_ot))
    {
      if (left_ot->ot_super == ot->ot_super && !(JP_NO_REIMPORT & jp->jp_join_flags))
	return JP_NO_JOIN;
      /* the probe cannot be imported into the build if the probe and build are on the same level.  In these cases, always better to reverse join order */
      rc |= JP_NO_REIMPORT;
    }

  if (dk_set_member (so->so_hash_probes, (void *) ot))
    {
      if (ot->ot_super == left_ot->ot_super && !(JP_NO_REIMPORT & jp->jp_join_flags))
	{
	  if (jp->jp_top_jp->jp_is_dt)
	    return JP_NO_JOIN;
	}
      rc |= JP_NO_REIMPORT;
    }
  rc |= (ot->ot_super != left_ot->ot_super || ot->ot_super != top_jp->jp_tb_dfe->_.table.ot->ot_super) ? JP_EXISTS : JP_JOIN;
  if (ot->ot_dfe->dfe_is_placed)
    rc |= JP_REUSE;
  return rc;
}


int
sqlo_is_same_level_probe (sqlo_t * so, df_elt_t * joined)
{
  /* the direct probe should not be imported to restrict a build side if on the same level.  Reversing the join order is always better.  But if the build is in a subq and reversing si not possible then the probe can sometimes be imported to restrict the build, e.g. ytpch q17 */
  op_table_t *probe_ot;
  dk_set_t mem = dk_set_member (so->so_hash_probes, (void *) joined->_.table.ot);
  if (!mem)
    return 0;
  probe_ot = (op_table_t *) mem->data;
  return joined->_.table.ot->ot_super == probe_ot->ot_super;
}


void
sqlo_jp_all_joins (sqlo_t * so, dk_set_t * all_jps, int hash_set)
{
  join_plan_t *top_jp = NULL;
  dk_hash_t *visited = hash_table_allocate (11);
  dk_set_t new_jps = *all_jps, save_hf_dfes;
  dk_set_t added_jps = NULL;
  DO_SET (join_plan_t *, jp, all_jps)
  {
    top_jp = jp->jp_top_jp;
    sethash ((void *) jp->jp_tb_dfe, visited, (void *) jp);
  }
  END_DO_SET ();
  while (new_jps)
    {
      DO_SET (join_plan_t *, jp, &new_jps)
      {
	join_plan_t *top_jp = jp->jp_top_jp;
	int jinx;
	for (jinx = 0; jinx < jp->jp_n_joined; jinx++)
	  {
	    join_plan_t *jp2;
	    df_elt_t *joined = jp->jp_joined[jinx];
	    if (0 && sqlo_is_same_level_probe (so, joined))
	      continue;
	    if (gethash ((void *) joined, visited))
	      continue;

	    jp2 = (join_plan_t *) t_alloc (sizeof (join_plan_t));
	    memzero (jp2, sizeof (join_plan_t));
	    sethash ((void *) joined, visited, (void *) jp2);
	    jp2->jp_top_jp = jp->jp_top_jp;
	    t_set_push (&added_jps, (void *) jp2);
	    t_set_push (all_jps, (void *) jp2);
	    save_hf_dfes = top_jp->jp_hash_fill_dfes;
	    t_set_pushnew (&top_jp->jp_hash_fill_dfes, (void *) joined);
	    jp2->jp_tb_dfe = joined;
	    joined->_.table.ot->ot_jp = jp2;
	    dfe_jp_fill (so, so->so_this_dt, joined, jp2, JPF_TRY, hash_set);
	    if (jp2->jp_not_for_hash_fill)
	      {
		added_jps = added_jps->next;
		*all_jps = (*all_jps)->next;
		top_jp->jp_hash_fill_dfes = save_hf_dfes;
	      }
	    jp_expand_eq_set (so, jp2);
	  }
      }
      END_DO_SET ();
      new_jps = added_jps;
      added_jps = NULL;
    }
  hash_table_free (visited);
}


void
sqlo_jp_find_path (sqlo_t * so, join_plan_t * jp, dk_set_t path, dk_hash_t * visited)
{
  /* find the shortest path to one of the tables in the hash filler, add the jps on that path */
  int jinx;
  join_plan_t *top_jp = jp->jp_top_jp;
  s_node_t node;
  node.next = path;
  node.data = jp;
  for (jinx = 0; jinx < jp->jp_n_joined; jinx++)
    {
      df_elt_t *joined = jp->jp_joined[jinx];
      if (gethash ((void *) joined, visited))
	continue;
      if (dk_set_member (top_jp->jp_probe_ots, joined->_.table.ot))
	continue;
      sethash ((void *) joined, visited, (void *) 1);
      if ((JP_NO_JOIN & joined->_.table.ot->ot_jp->jp_join_flags))
	continue;
      if (dk_set_member (top_jp->jp_hash_fill_dfes, (void *) joined))
	{
	  int plen = dk_set_length (&node);
	  if (!top_jp->jp_path || plen < dk_set_length (top_jp->jp_path))
	    top_jp->jp_path = t_set_copy (&node);
	}
      else
	sqlo_jp_find_path (so, joined->_.table.ot->ot_jp, &node, visited);
    }
}


float
sqlo_jp_path_card (join_plan_t * top_jp)
{
  float c = 1;
  DO_SET (join_plan_t *, jp, &top_jp->jp_path) c *= jp->jp_fanout;
  END_DO_SET ();
  return c;
}


int
sqlo_jp_add_path (sqlo_t * so, join_plan_t * jp)
{
  int rc;
  float path_card;
  dk_hash_t *visited = hash_table_allocate (11);
  join_plan_t *top_jp = jp->jp_top_jp;
  top_jp->jp_path = NULL;
  sqlo_jp_find_path (so, jp, NULL, visited);
  hash_table_free (visited);
  if (!top_jp->jp_path)
    return 0;
  path_card = sqlo_jp_path_card (top_jp);
  if (path_card > 0.8)
    return 0;
  DO_SET (join_plan_t *, jp, &top_jp->jp_path)
  {
    top_jp->jp_best_card *= jp->jp_fanout;
    if ((JP_EXISTS & jp->jp_join_flags))
      {
	t_set_push (&top_jp->jp_hash_fill_exists, (void *) sqlo_restr_as_exists (so, iter));
	break;
      }
    t_set_pushnew (&top_jp->jp_hash_fill_dfes, (void *) jp->jp_tb_dfe);
  }
  END_DO_SET ();
  return 1;
}


void
jp_placeable_fanout (join_plan_t * jp)
{
  int inx;
  for (inx = 0; inx < jp->jp_n_preds; inx++)
    jp->jp_preds[inx].ps_is_placeable = jp_reqd_placed (jp, jp->jp_preds[inx].ps_pred, jp->jp_tb_dfe);
  jp_fanout (jp);
}

void
sqlo_jp_expand (sqlo_t * so, dk_set_t initial)
{
  dk_hash_t *visited = hash_table_allocate (11);
  dk_set_t border = initial;
  dk_set_t new_border = initial;
  while (border)
    {
      DO_SET (join_plan_t *, start_jp, &border)
      {
	join_plan_t *top_jp = start_jp->jp_top_jp;
	int jinx;
	for (jinx = 0; jinx < start_jp->jp_n_joined; jinx++)
	  {
	    df_elt_t *joined = start_jp->jp_joined[jinx];
	    join_plan_t *jp = joined->_.table.ot->ot_jp;
	    if (dk_set_member (so->so_hash_probes, (void *) joined->_.table.ot)
		&& start_jp->jp_tb_dfe->_.table.ot->ot_super == joined->_.table.ot->ot_super)
	      continue;		/* do not expand via a direct probe if the direct probe is on the same level */
	    if (!jp || (JP_NO_JOIN & jp->jp_join_flags))
	      continue;
	    if (gethash ((void *) jp, visited))
	      continue;
	    jp_placeable_fanout (jp);
	    if (jp->jp_fanout > 1.1)
	      continue;
	    if (jp->jp_fanout < 0.8)
	      {
		/* a restriction can be added.  It may be added as an exists or as a join.  If a join, then add to the new broder, if exists leave it as is. */
		dk_set_t ex = top_jp->jp_hash_fill_exists;
		if (!sqlo_jp_add_path (so, jp))
		  continue;
		if (ex == top_jp->jp_hash_fill_exists)
		  t_set_push (&new_border, (void *) jp->jp_tb_dfe);

	      }
	    sethash ((void *) jp, visited, (void *) 1);
	  }
      }
      END_DO_SET ();
      border = new_border;
      new_border = NULL;
    }
  hash_table_free (visited);
}


void
sqlo_jp_build_init (sqlo_t * so, df_elt_t * hash_ref_tb, dk_set_t * all_jps, dk_set_t org_preds, dk_set_t hash_keys, int hash_set)
{
  /* Put the table(s) in hash ref dfe in jp.  This can be table or derived table that is refd.  This imports further joins into build */
  op_table_t *ot = so->so_this_dt;
  if (DFE_DT == hash_ref_tb->dfe_type)
    {
      join_plan_t *top_jp;
      DO_SET (df_elt_t *, dfe, &hash_ref_tb->_.sub.ot->ot_from_dfes)
      {
	sqlo_jp_build_init (so, dfe, all_jps, org_preds, hash_keys, hash_set);
      }
      END_DO_SET ();
      top_jp = ((join_plan_t *) (*all_jps)->data)->jp_top_jp;
      top_jp->jp_is_dt = 1;
    }
  else
    {
      t_NEW_VARZ (join_plan_t, jp);
      if (!*all_jps)
	{
	  jp->jp_top_jp = jp;
	  jp->jp_probe_ots = so->so_hash_probes;
	}
      else
	jp->jp_top_jp = ((join_plan_t *) (*all_jps)->data)->jp_top_jp;
      t_set_push (all_jps, (void *) jp);
      t_set_push (&jp->jp_top_jp->jp_all_joined, (void *) jp);
      hash_ref_tb->_.table.ot->ot_jp = jp;
      dfe_jp_fill (so, ot, hash_ref_tb, jp, JPF_TRY, hash_set);
      jp_expand_eq_set (so, jp);
      jp->jp_in_hash_fill = 1;
      t_set_push (&jp->jp_top_jp->jp_hash_fill_dfes, (void *) hash_ref_tb);
      jp->jp_fill_selectivity = jp->jp_fanout / dfe_scan_card (hash_ref_tb);
    }
}


int
sqlo_pred_jp_placeable (join_plan_t * top_jp, df_elt_t * pred)
{
  DO_SET (op_table_t *, dep, &pred->dfe_tables)
  {
    DO_SET (df_elt_t *, fill_dfe, &top_jp->jp_hash_fill_dfes)
    {
      if (dep == fill_dfe->_.table.ot)
	goto found;
    }
    END_DO_SET ();
    return 0;
  found:;
  }
  END_DO_SET ();
  return 1;
}


void
sqlo_jp_hash_fill_preds (sqlo_t * so, join_plan_t * top_jp)
{
  DO_SET (df_elt_t *, fill_dfe, &top_jp->jp_hash_fill_dfes)
  {
    join_plan_t *jp = fill_dfe->_.table.ot->ot_jp;
    int inx;
    for (inx = 0; inx < jp->jp_n_preds; inx++)
      {
	df_elt_t *pred = jp->jp_preds[inx].ps_pred;
	if (!sqlo_pred_jp_placeable (top_jp, pred))
	  continue;
	if (dfe_is_eq_pred (pred))
	  {
	    DO_SET (df_elt_t *, previous, &top_jp->jp_hash_fill_preds)
	    {
	      if (dfe_is_eq_pred (previous)
		  && ((pred->_.bin.left == previous->_.bin.right && pred->_.bin.right == previous->_.bin.left)
		      || (pred->_.bin.left == previous->_.bin.left && pred->_.bin.right == previous->_.bin.right)))
		goto already_in;
	    }
	    END_DO_SET ();
	  }
	t_set_pushnew (&top_jp->jp_hash_fill_preds, (void *) pred);
      already_in:;
      }
  }
  END_DO_SET ();
}


ST **sqlo_gby_hf_sel (sqlo_t * so, ST ** sel, dk_set_t hash_keys);


ST *
sqlo_dt_hash_selection (sqlo_t * so, df_elt_t * hash_ref_tb, ST * sel, dk_set_t hash_keys)
{
  ST *dt = hash_ref_tb->_.sub.ot->ot_dt;

  return (ST *) (sel->_.select_stmt.selection =
      (caddr_t *) sqlo_gby_hf_sel (so, (ST **) t_box_copy_tree ((caddr_t) dt->_.select_stmt.selection), hash_keys));
}


int
sqlo_hash_fill_join (sqlo_t * so, df_elt_t * hash_ref_tb, df_elt_t ** fill_ret, dk_set_t org_preds, dk_set_t hash_keys,
    float ref_card)
{
  /* find unplaced tables that join to hash_ef tb, restricting card and do not join to any other placed thing */
  int inx, ctr = 0;
  char sqk[SQK_MAX_CHARS];
  char *p_sqk = sqk;
  int hash_set = unbox (sqlo_opt_value (hash_ref_tb->_.table.ot->ot_opts, OPT_HASH_SET));
  int sqk_fill = 0;
  ST **hf_org_sel = NULL;
  df_elt_t **sqc_place = NULL;
  df_elt_t *fill_copy;
  dk_set_t all_jps = NULL, init_jps, init_dfes;
  df_elt_t *fill_dfe;
  join_plan_t *top_jp;
  if (!enable_hash_fill_join || -1 == hash_set)
    return 0;
  sqlo_jp_build_init (so, hash_ref_tb, &all_jps, org_preds, hash_keys, hash_set);
  top_jp = ((join_plan_t *) all_jps->data)->jp_top_jp;
  init_dfes = top_jp->jp_hash_fill_dfes;
  top_jp->jp_hash_fill_preds = org_preds;
  top_jp->jp_hash_fill_non_unq = DFE_TABLE == hash_ref_tb->dfe_type && !hash_ref_tb->_.table.is_unique;
  top_jp->jp_best_card = 1;
  init_jps = t_set_copy (all_jps);
  sqlo_jp_all_joins (so, &all_jps, hash_set);
  top_jp->jp_hash_fill_dfes = init_dfes;
  sqlo_jp_expand (so, init_jps);
  if (!top_jp->jp_n_joined && DFE_TABLE == hash_ref_tb->dfe_type)
    return 0;
  if (top_jp->jp_best_card > 0.9 && !hash_set && DFE_TABLE == hash_ref_tb->dfe_type)
    return 0;
  if (ref_card <= top_jp->jp_fanout * top_jp->jp_best_card && !hash_set && DFE_TABLE == hash_ref_tb->dfe_type
      && !top_jp->jp_hash_fill_exists)
    return 0;			/* the build is larger than the probe, reverse order bound to be better  */
  if (2 == enable_hash_fill_join)
    return 0;
  sqlo_jp_hash_fill_preds (so, top_jp);
  if (so->so_cache_subqs)
    {
      dfe_cc_list_key (hash_keys, sqk, &sqk_fill, sizeof (sqk) - 1);
      sprintf_more (sqk, sizeof (sqk), &sqk_fill, "| ");
      dfe_cc_list_key (top_jp->jp_hash_fill_dfes, sqk, &sqk_fill, sizeof (sqk) - 1);
      dfe_cc_list_key (top_jp->jp_hash_fill_preds, sqk, &sqk_fill, sizeof (sqk) - 1);
      so_ensure_subq_cache (so);
      if (sqk_fill < sizeof (sqk) - 10)
	sqc_place = (df_elt_t **) id_hash_get (so->so_subq_cache, (caddr_t) & p_sqk);
    }
  if (sqc_place)
    {
      fill_copy = sqlo_layout_copy (so, *sqc_place, hash_ref_tb);
    }
  else
    {
      ST **from = (ST **) t_list_to_array (top_jp->jp_hash_fill_dfes);
      ST *pred_tree = NULL;
      ST *texp = t_listst (9, TABLE_EXP, from,
	  NULL, NULL, NULL, NULL, NULL, NULL, NULL);
      ST *sel = t_listst (5, SELECT_STMT, NULL, t_list_to_array (hash_keys), NULL, texp);
      op_table_t *fill_ot;
      DO_SET (df_elt_t *, dfe, &top_jp->jp_hash_fill_preds) t_st_and (&pred_tree, sqlo_pred_tree (dfe));
      END_DO_SET ();
      DO_SET (ST *, exists, &top_jp->jp_hash_fill_exists) t_st_and (&pred_tree, exists);
      END_DO_SET ();
      texp->_.table_exp.where = pred_tree;
      DO_BOX (df_elt_t *, dfe, inx, sel->_.select_stmt.selection)
      {
	ST *as;
	char tmp[MAX_QUAL_NAME_LEN];
	if (DFE_COLUMN == dfe->dfe_type)
	  snprintf (tmp, sizeof (tmp), "%s.%s", dfe->dfe_tree->_.col_ref.prefix, dfe->dfe_tree->_.col_ref.name);
	else
	  snprintf (tmp, sizeof (tmp), "h%d", ctr++);
	as = t_listst (5, BOP_AS, t_box_copy_tree ((caddr_t) dfe->dfe_tree), NULL, t_box_string (tmp), NULL);
	sel->_.select_stmt.selection[inx] = (caddr_t) as;
      }
      END_DO_BOX;
      if (DFE_DT == hash_ref_tb->dfe_type)
	hf_org_sel = (ST **) t_box_copy_tree ((caddr_t) sqlo_dt_hash_selection (so, hash_ref_tb, sel, hash_keys));
      DO_BOX (df_elt_t *, tb_dfe, inx, texp->_.table_exp.from)
      {
	caddr_t *opts = t_list (2, OPT_JOIN_RESTR, tb_dfe->_.table.ot->ot_jp->jp_join_flags);
	texp->_.table_exp.from[inx] =
	    t_listst (3, TABLE_REF, t_listst (6, TABLE_DOTTED, tb_dfe->_.table.ot->ot_table->tb_name,
		tb_dfe->_.table.ot->ot_new_prefix, NULL, NULL, opts), NULL);

      }
      END_DO_BOX;
      sel = t_box_copy_tree ((caddr_t) sel);
      sqlo_scope (so, &sel);
      fill_dfe = sqlo_df (so, sel);
      fill_ot = fill_dfe->_.sub.ot;
      fill_ot->ot_dfe = fill_dfe;
      sqlo_eqs (so, fill_ot);
      fill_dfe->dfe_super = hash_ref_tb;
      fill_ot->ot_work_dfe = dfe_container (so, DFE_DT, hash_ref_tb);
      fill_ot->ot_work_dfe->_.sub.in_arity = 1;
      fill_ot->ot_work_dfe->_.sub.hash_filler_of = hash_ref_tb;
      fill_copy = sqlo_layout (so, fill_ot, SQLO_LAY_VALUES, hash_ref_tb);
      fill_copy->_.sub.is_hash_filler_unique = hash_ref_tb->_.table.is_unique && !top_jp->jp_hash_fill_non_unq;
      fill_copy->_.sub.hash_filler_of = hash_ref_tb;
      fill_copy->_.sub.n_hash_fill_keys = dk_set_length (hash_keys);
      fill_copy->_.sub.hash_fill_org_sel = hf_org_sel;
      if (so->so_cache_subqs)
	{
	  caddr_t k = t_box_string (sqk);
	  df_elt_t *cp = sqlo_layout_copy (so, fill_copy, NULL);
	  t_id_hash_set (so->so_subq_cache, (caddr_t) & k, (caddr_t) & cp);
	}
    }
  if (DFE_TABLE == hash_ref_tb->dfe_type)
    hash_ref_tb->_.table.hash_filler = fill_copy;
  else
    hash_ref_tb->_.sub.hash_filler = fill_copy;
  *fill_ret = fill_copy;
  DO_SET (df_elt_t *, pred, &top_jp->jp_hash_fill_preds)
  {
    if (pred->dfe_is_placed)
      t_set_push (&fill_copy->_.sub.hash_fill_reused_tables, (void *) pred);
  }
  END_DO_SET ();
  DO_SET (df_elt_t *, tb_dfe, &top_jp->jp_hash_fill_dfes)
  {
    if (tb_dfe == hash_ref_tb)
      continue;
    if (tb_dfe->dfe_is_placed)
      t_set_push (&fill_copy->_.sub.hash_fill_reused_tables, (void *) tb_dfe);
    else
      {
	t_set_push (&fill_copy->_.sub.dt_preds, (void *) tb_dfe);
	tb_dfe->dfe_is_placed = 1;
      }
  }
  END_DO_SET ();
  top_jp->jp_hash_fill_preds = dk_set_conc (top_jp->jp_extra_preds, top_jp->jp_hash_fill_preds);
  DO_SET (df_elt_t *, dfe, &top_jp->jp_hash_fill_preds)
  {
    if (!dk_set_member (org_preds, (void *) dfe) && !dfe->dfe_is_placed)
      t_set_pushnew (&fill_copy->_.sub.dt_preds, (void *) dfe);
    dfe->dfe_is_placed = 1;
  }
  END_DO_SET ();
  fill_copy->_.sub.hash_fill_imp_sel = top_jp->jp_best_card;
  return 1;
}


void
dfe_unplace_fill_join (df_elt_t * fill_dt, df_elt_t * tb_dfe, dk_set_t org_preds)
{
  if (DFE_DT != fill_dt->dfe_type)
    return;
  DO_SET (df_elt_t *, elt, &fill_dt->_.sub.dt_preds)
  {
    if (dk_set_member (org_preds, (void *) elt))
      continue;
    elt->dfe_is_placed = 0;
  }
  END_DO_SET ();
  if (0 && tb_dfe)
    {
      DO_SET (df_elt_t *, elt, &tb_dfe->_.table.all_preds) elt->dfe_is_placed = 1;
      END_DO_SET ();
      DO_SET (df_elt_t *, elt, &tb_dfe->_.table.all_preds) elt->dfe_is_placed = 1;
      END_DO_SET ();
    }
}


/* Cache compilations of subqs, dts and hash fill dts */

void
dfe_cc_key (df_elt_t * dfe, char *str, int *fill, int space)
{
  if (!dfe)
    return;
  switch (dfe->dfe_type)
    {
    case DFE_TABLE:
    case DFE_DT:
    case DFE_EXISTS:
    case DFE_VALUE_SUBQ:
      sprintf_more (str, space, fill, "%s", dfe->_.table.ot->ot_new_prefix);
      break;
    case DFE_BOP:
    case DFE_BOP_PRED:
      dfe_cc_key (dfe->_.bin.left, str, fill, space);
      sprintf_more (str, space, fill, " %d", dfe->_.bin.op);
      dfe_cc_key (dfe->_.bin.right, str, fill, space);
      break;
    case DFE_COLUMN:
      sprintf_more (str, space, fill, "%s.%s", dfe->dfe_tree->_.col_ref.prefix, dfe->dfe_tree->_.col_ref.name);
      break;
    }
}


void
dfe_cc_list_key (dk_set_t list, char *str, int *fill, int space)
{
  DO_SET (df_elt_t *, dfe, &list) dfe_cc_key (dfe, str, fill, space);
  END_DO_SET ();
}


void
so_ensure_subq_cache (sqlo_t * so)
{
  if (!so->so_subq_cache)
    so->so_subq_cache = t_id_hash_allocate (101, sizeof (caddr_t), sizeof (caddr_t), strhash, strhashcmp);
}

#define CAN_CACHE(xx) { if (dfe_pred_body_cacheable (dfe->_.xx)) return 0;}


int
dfe_pred_body_cacheable (df_elt_t ** body)
{
  int inx;
  int first;
  if (!body)
    return 1;
  first = (int) (ptrlong) body[0];
  DO_BOX (df_elt_t **, elt, inx, body)
  {
    if (!IS_BOX_POINTER (elt))
      continue;
    if (DFE_PRED_BODY == first)
      {
	if (dfe_is_cacheable (body[inx]))
	  return 0;
      }
    else
      {
	if (!dfe_pred_body_cacheable (elt))
	  return 0;
      }
  }
  END_DO_BOX;
  return 1;
}


int
dfe_is_cacheable (df_elt_t * dfe)
{
  if (!dfe)
    return 1;
  switch (dfe->dfe_type)
    {
    case DFE_DT:
    case DFE_PRED_BODY:
    case DFE_VALUE_SUBQ:
    case DFE_EXISTS:
      if (dfe->_.sub.generated_dfe
	  && (DFE_DT == dfe->_.sub.generated_dfe->dfe_type || DFE_VALUE_SUBQ == dfe->_.sub.generated_dfe->dfe_type))
	return dfe_is_cacheable (dfe->_.sub.generated_dfe);
      else
	{
	  df_elt_t *s;
	  for (s = dfe->_.sub.first; s; s = s->dfe_next)
	    if (!dfe_is_cacheable (s))
	      return 0;
	  return 1;
	}
    case DFE_QEXP:
      {
	int inx;
	DO_BOX (df_elt_t *, term, inx, dfe->_.qexp.terms)
	{
	  if (!dfe_is_cacheable (term))
	    return 0;
	}
	END_DO_BOX;
	return 1;
      }

    case DFE_TABLE:
      {
	CAN_CACHE (table.join_test);
	CAN_CACHE (table.after_join_test);
	CAN_CACHE (table.vdb_join_test);
	if (dfe->_.table.hash_filler_reuse)
	  return 0;
	if (dfe->_.table.hash_filler)
	  {
	    if (!dfe_is_cacheable (dfe->_.table.hash_filler))
	      return 0;
	  }
	CAN_CACHE (table.hash_filler_after_code);
	return 1;
      }
    case DFE_BOP:
    case DFE_BOP_PRED:
    case DFE_CALL:
      return 1;
    case DFE_REUSE:
      return 0;
    case DFE_FILTER:
      CAN_CACHE (filter.body);
      return 1;

    case DFE_GROUP:
    case DFE_ORDER:
      return 1;

    case DFE_CONTROL_EXP:
      {
	int inx;
	DO_BOX (df_elt_t **, elt, inx, dfe->_.control.terms)
	{
	  CAN_CACHE (control.terms[inx]);
	}
	END_DO_BOX;
	return 1;
      }

    case DFE_TEXT_PRED:
    case DFE_HEAD:
      return 1;
    default:
      return 0;
    }
  return 0;			/*dummy */
}


df_elt_t *
sqlo_dt_cache_lookup (sqlo_t * so, op_table_t * ot, dk_set_t imp_preds, caddr_t * cc_key_ret)
{
  char sqk[SQK_MAX_CHARS];
  caddr_t p_sqk = sqk;
  df_elt_t **place;
  int sqk_fill;
  if (!enable_subq_cache || !so->so_cache_subqs)
    {
      *cc_key_ret = NULL;
      return NULL;
    }
  so_ensure_subq_cache (so);
  snprintf (sqk, sizeof (sqk), "D %s", ot->ot_new_prefix);
  sqk_fill = strlen (sqk);
  dfe_cc_list_key (imp_preds, sqk, &sqk_fill, sizeof (sqk) - 1);
  if (sqk_fill > SQK_MAX_CHARS - 5)
    {
      *cc_key_ret = NULL;
      return NULL;
    }
  place = (df_elt_t **) id_hash_get (so->so_subq_cache, (caddr_t) & p_sqk);
  if (!place)
    {
      *cc_key_ret = t_box_string (sqk);
      return NULL;
    }
  *cc_key_ret = NULL;
  return sqlo_layout_copy (so, *place, NULL);
}


void
sqlo_dt_cache_add (sqlo_t * so, caddr_t cc_key, df_elt_t * copy)
{
  if (!dfe_is_cacheable (copy))
    return;
  copy = sqlo_layout_copy (so, copy, NULL);
  t_id_hash_set (so->so_subq_cache, (caddr_t) & cc_key, (caddr_t) & copy);
}




#define DTH_NO_BUILD 1


#define GBY_KEY 1
#define GBY_DEP 2
#define GBY_AGG 3
#define GBY_DEP_EXP 4





int
col_agg_status (df_elt_t * dt, df_elt_t * exp)
{
  sqlo_t *so = dt->dfe_sqlo;
  int inx;
  if (DFE_COLUMN != exp->dfe_type)
    return GBY_DEP_EXP;
  ST *sel = dt->_.sub.ot->ot_dt;
  df_elt_t *gby = dt->_.sub.ot->ot_group_dfe;
  if (!gby)
    return GBY_KEY;
  inx = st_col_index (so, dt->_.sub.ot, exp->dfe_tree->_.col_ref.name);
  exp = sqlo_df (so, (ST *) sel->_.select_stmt.selection[inx]);
  if (DFE_COLUMN != exp->dfe_type)
    return GBY_DEP_EXP;
  if (dk_set_member (gby->_.setp.gb_dependent, (void *) exp))
    return GBY_DEP_EXP;
  return GBY_KEY;
}


int
sqlo_hdt_aggs (ST * tree, dk_set_t * res)
{
  if (ST_P (tree, FUN_REF))
    {
      char alias[10];
      caddr_t alias_box;
      snprintf (alias, sizeof (alias), "a%d", dk_set_length (*res));
      alias_box = t_box_string (alias);
      tree->_.fn_ref.fn_name = alias_box;
      t_set_push (res, (void *) t_list (6, BOP_AS, tree, NULL, alias_box, NULL, NULL));
      return 1;
    }
  return 0;
}

ST *
sqlo_find_as (ST ** sel, caddr_t alias, int *pos_ret)
{
  int inx;
  DO_BOX (ST *, exp, inx, sel)
  {
    if (ST_P (exp, BOP_AS) && box_equal (exp->_.as_exp.name, alias))
      {
	*pos_ret = inx;
	return exp;
      }
  }
  END_DO_BOX;
  SQL_GPF_T1 (top_sc->sc_cc, "no alias found in hash build side dt");
  return NULL;
}


ST **
sqlo_gby_hf_sel (sqlo_t * so, ST ** sel, dk_set_t hash_keys)
{
  int inx;
  dk_set_t cols = NULL, keys = NULL, key_pos = NULL;
  DO_SET (df_elt_t *, key, &hash_keys)
  {
    int pos;
    ST *exp = sqlo_find_as (sel, key->dfe_tree->_.col_ref.name, &pos);
    keys = dk_set_conc (keys, t_cons (exp, NULL));
    t_set_push (&key_pos, (void *) (ptrlong) pos);
  }
  END_DO_SET ();

  DO_BOX (ST *, exp, inx, sel)
  {
    dk_set_t res = NULL;
    if (dk_set_member (key_pos, (void *) (ptrlong) inx))
      continue;
    sqlo_map_st (exp, (tree_cb_t) sqlo_hdt_aggs, &res);
    if (!res)
      cols = dk_set_conc (cols, t_cons ((void *) exp, NULL));
    else
      {
	cols = dk_set_conc (cols, dk_set_nreverse (res));
      }
  }
  END_DO_BOX;
  return (ST **) t_list_to_array (dk_set_conc (keys, cols));
}



void
sqlo_dt_gby_result (sqlo_t * so, df_elt_t * ref_dfe, df_elt_t * fill_dfe, ST *** sel_place, ST ** sel_save)
{
  /* if a dt is joined by hash and has gby aggs as result and there is in the dt an exp on the aggs then this exp must be placed after the hash ref */
  int old_mode = so->so_place_code_forr_cond, inx;
  df_elt_t *old_pt = so->so_gen_pt;
  dk_set_t hash_out = NULL;
  df_elt_t *ref_container;
  caddr_t prefix = ref_dfe->_.sub.ot->ot_new_prefix;
  int n_keys = dk_set_length (ref_dfe->_.sub.hash_keys);
  ST **sel = (ST **) ref_dfe->_.sub.ot->ot_dt->_.select_stmt.selection;
  *sel_place = (ST **) & ref_dfe->_.sub.ot->ot_dt->_.select_stmt.selection;
  *sel_save = t_box_copy_tree (sel);
  DO_BOX (ST *, fref, inx, fill_dfe->_.sub.hash_fill_org_sel)
  {
    if (inx < n_keys)
      {
	t_set_push (&hash_out, NULL);
	continue;
      }
    if (ST_P (fref, BOP_AS) && ST_P (fref->_.as_exp.left, FUN_REF))
      {
	ST *st_fref = fref->_.as_exp.left;
	caddr_t fn_name = fref->_.fn_ref.fn_name;
	ST *col_ref = t_listst (3, COL_DOTTED, prefix, fref->_.as_exp.name);
	df_elt_t *fref_col = sqlo_df (so, col_ref);
	t_set_push (&hash_out, (void *) fref_col);
	t_set_push (&ref_dfe->dfe_defines, (void *) fref_col);

	/* the agg has fn_name set to the alias.  To match the ref in the org select it must be the dt cname of the dt with the fref */
	st_fref->_.fn_ref.fn_name = prefix;
	sqlo_replace_tree ((ST **) & sel, st_fref, (caddr_t) col_ref);
	st_fref->_.fn_ref.fn_name = fn_name;
      }
    else
      {
	ST *col_ref = t_listst (3, COL_DOTTED, prefix, fref->_.as_exp.name);
	df_elt_t *fref_col = sqlo_df (so, col_ref);
	t_set_push (&hash_out, (void *) fref_col);
	t_set_push (&ref_dfe->dfe_defines, (void *) fref_col);
      }
  }
  END_DO_BOX;
  ref_container = dfe_container (so, DFE_PRED_BODY, ref_dfe);
  so->so_gen_pt = ref_container->_.sub.first;
  so->so_place_code_forr_cond = 1;
  DO_BOX (ST *, exp, inx, sel)
  {
    caddr_t as_name = NULL;
    if (ST_P (exp, BOP_AS))
      {
	as_name = exp->_.as_exp.name;
	exp = exp->_.as_exp.left;
      }
    if (!ST_P (exp, COL_DOTTED))
      {
	df_elt_t *defd = sqlo_df (so, t_listst (3, COL_DOTTED, ref_dfe->_.sub.ot->ot_new_prefix, as_name));
	df_elt_t *defining = sqlo_df (so, exp);
	sqlo_place_exp (so, ref_container, defining);
	t_set_push (&ref_dfe->dfe_defines, defd->dfe_tree);
	t_set_push (&ref_dfe->_.sub.hash_ref_defs, (void *) defining->dfe_tree);
	t_set_push (&ref_dfe->_.sub.hash_ref_defs, (void *) defd->dfe_tree);
      }
  }
  END_DO_BOX;
  so->so_gen_pt = old_pt;
  so->so_place_code_forr_cond = old_mode;
  ref_dfe->_.sub.hash_ref_after_code = df_body_to_array (ref_container);
  ref_dfe->_.sub.dt_hash_out = (df_elt_t **) t_list_to_array (dk_set_nreverse (hash_out));
}



int
sqlo_dth_need_build (df_elt_t * dt)
{
  /* a group by dt needs no separate hash build, other dts do */
  df_elt_t *elt;
  df_elt_t *gby = NULL, *oby = NULL;
  ST *sel = dt->_.sub.ot->ot_dt;
  if (ST_P (sel, SELECT_STMT) && SEL_TOP (sel))
    return 1;
  for (elt = dt->_.sub.first; elt; elt = elt->dfe_next)
    {
      if (DFE_GROUP == elt->dfe_type)
	gby = elt;
      else if (DFE_ORDER == elt->dfe_type)
	oby = elt;
    }
  return (gby && !oby);
}

void
sqlo_dt_hash_tried (df_elt_t * dfe)
{
  /* the placed flags of the tables in the hash joined dt should be reset, else next attempt to place the dt will not place the tables.  Tables imported into the dt will on the other hand stay placed and will be unplaced when the dt itself is */
  DO_SET (df_elt_t *, from, &dfe->_.sub.ot->ot_from_dfes) from->dfe_is_placed = 0;
  END_DO_SET ();
}

dk_set_t
st_set_copy (dk_set_t s)
{
  dk_set_t cp = t_set_copy (s), n;
  for (n = cp; n; n = n->next)
    n->data = t_box_copy_tree ((caddr_t) n->data);
  return cp;
}


df_elt_t *
sqlo_dt_try_hash (sqlo_t * so, df_elt_t * dfe, op_table_t * super_ot, float *score_ret, df_elt_t * loop_dt)
{
  ST **sel_place = NULL, *save_sel = NULL;
  dk_set_t hash_pred_locus_refs = NULL;
  float size_est = 0;
  dk_set_t prev_probes = so->so_hash_probes;
  dk_set_t preds = dfe->_.sub.ot->ot_is_outer ? dfe->_.table.ot->ot_join_preds : dfe->_.table.all_preds;
  df_elt_t *dt_dfe = dfe;
  float fill_unit, fill_arity, ref_arity;
  dk_set_t hash_refs = NULL, hash_keys = NULL;
  df_elt_t *fill_dfe;
  int need_build, fill_join;
  dk_set_t org_preds = NULL, post_preds = NULL;
  int mode, has_non_inv_key = 0, dt_mode;
  dfe_reuse_t *dfr = NULL;
  op_table_t *ot = dfe->_.sub.ot;
  mode = (int) (ptrlong) sqlo_opt_value (ot->ot_opts, OPT_JOIN);
  dt_mode = (int) (ptrlong) sqlo_opt_value (super_ot->ot_opts, OPT_JOIN);
  if (!mode)
    mode = dt_mode;
  ref_arity = dfe_arity_with_supers (dfe->dfe_prev);
  if (!hash_join_enable || (mode && OPT_HASH != mode))
    return 0;
  DO_SET (df_elt_t *, pred, &preds)
  {
    if (dfe_is_tb_only (pred, ot))
      t_set_push (&org_preds, (void *) pred);
    else if (dfe_is_eq_pred (pred))
      {
	df_elt_t *left = pred->_.bin.left;
	df_elt_t *right = pred->_.bin.right;
	if (dfe_is_tb_only (left, ot) && !dk_set_member (right->dfe_tables, (void *) ot))
	  {
	    if (GBY_KEY == col_agg_status (dfe, left))
	      {
		if (right->dfe_tables)
		  has_non_inv_key = 1;
		t_set_push (&hash_keys, (void *) left);
		t_set_push (&hash_refs, (void *) right);
		sqt_types_set (&(left->dfe_sqt), &(right->dfe_sqt));
		hash_pred_locus_refs = t_set_union (pred->dfe_remote_locus_refs, hash_pred_locus_refs);
	      }
	    else
	      t_set_push (&post_preds, (void *) pred);
	  }
	else if (dfe_is_tb_only (right, ot) && !dk_set_member (left->dfe_tables, (void *) ot))
	  {
	    if (GBY_KEY == col_agg_status (dfe, right))
	      {
		if (left->dfe_tables)
		  has_non_inv_key = 1;
		t_set_push (&hash_keys, (void *) right);
		t_set_push (&hash_refs, (void *) left);
		sqt_types_set (&(left->dfe_sqt), &(right->dfe_sqt));
		hash_pred_locus_refs = t_set_union (pred->dfe_remote_locus_refs, hash_pred_locus_refs);
	      }
	    else
	      t_set_push (&post_preds, (void *) pred);
	  }
	else
	  t_set_push (&post_preds, (void *) pred);
      }
    else
      t_set_push (&post_preds, (void *) pred);
  }
  END_DO_SET ();
  if (!hash_keys || !has_non_inv_key)
    return 0;

  if (!mode)
    {
      if (super_ot && ST_P (super_ot->ot_dt, SELECT_STMT) && !sqlo_is_postprocess (so, dfe->dfe_super, dfe))
	{			/*GK: TODO: make a better model to account for TOP */
	  ST *top_exp = SEL_TOP (super_ot->ot_dt);
	  ptrlong top_cnt = sqlo_select_top_cnt (so, top_exp);
	  if (top_cnt)
	    {
	      if (dfe->dfe_arity > 1)
		{
		  ref_arity = top_cnt;
		}
	      else if (dfe->dfe_arity < 1)
		{
		  ref_arity = top_cnt / dfe->dfe_arity;
		}
	    }
	}
      if (ref_arity < 1)
	return 0;
    }
  need_build = sqlo_dth_need_build (dfe);
  sqlo_record_probes (so, hash_refs);
  fill_join = sqlo_hash_fill_join (so, dfe, &fill_dfe, org_preds, hash_keys, ref_arity);
  so->so_hash_probes = prev_probes;
  if (fill_join)
    {
      fill_unit = fill_dfe->dfe_unit;
      fill_arity = fill_dfe->dfe_arity;
      if (need_build)
	fill_unit += sqlo_hash_ins_cost (dfe, fill_arity, hash_keys, &size_est);
      dfe->dfe_arity *= dfe_hash_fill_cond_card (dfe);
    }
  else
    {
      sqlo_dt_hash_tried (dfe);
      return 0;
    }
  if (!mode)
    {
      float nh_unit = loop_dt->dfe_unit - loop_dt->_.sub.hash_fill_ov;
      int is_large = nh_unit < 1000 && size_est > chash_space_avail * chash_per_query_pct / 100;
      if ((is_large
	      || nh_unit * ref_arity + loop_dt->_.sub.hash_fill_ov < fill_unit + ref_arity * sqlo_hash_ref_cost (dfe, fill_arity))
	  && !super_ot->ot_is_right_oj)
	{
	  /* hash is not better.  But if trying right oj plan, can only be by hash. */
	  {
	    dfe_unplace_fill_join (fill_dfe, dfe, org_preds);
	    sqlo_dt_hash_tried (dfe);
	    dfe->_.sub.hash_filler = NULL;
	    return NULL;
	  }
	}
    }
  dfe = sqlo_new_dfe (so, DFE_DT, NULL);
  dfe->dfe_super = dt_dfe;
  dfe->_.sub.reuses_hash_filler = dfr != NULL;
  dfe->_.sub.hash_filler_reuse = dfr;
  dfe->_.sub.hash_role = HR_REF;
  dfe->_.sub.ot = dt_dfe->_.sub.ot;
  if (!dfr)
    t_set_push (&so->so_top_ot->ot_hash_fillers, (void *) fill_dfe);
  dfe->_.sub.hash_filler = fill_dfe;
  dfe->dfe_unit = HASH_LOOKUP_COST + HASH_ROW_COST * MAX (0, dfe->dfe_arity - 1);
  fill_dfe->dfe_unit = fill_unit;
  dfe->_.sub.hash_refs = hash_refs;
  /* it can be that a dt filler has placed more tables.  If so, there can be new predicates that become defined, eg tpch q7 */
  sqlo_hash_fill_dt_new_preds (so, super_ot, dfe, &post_preds);
  sqlo_dt_gby_result (so, dfe, fill_dfe, &sel_place, &save_sel);
  dfe->_.sub.join_test = sqlo_and_list_body (so, LOC_LOCAL, dfe, post_preds);
  dfe->dfe_unit = 0;
  *score_ret = sqlo_score (super_ot->ot_work_dfe, super_ot->ot_work_dfe->_.sub.in_arity);
  if (dfe->_.sub.ot->ot_is_outer)
    {
      dk_set_t after = t_set_diff (dfe->_.sub.dt_preds, dfe->_.sub.ot->ot_join_preds);
      dfe->_.sub.after_join_test = sqlo_and_list_body (so, LOC_LOCAL, dfe, after);
    }
  fill_dfe->_.sub.gby_hash_filler = !need_build;
  dfr_done (so, dfr);
  sqlo_dt_hash_tried (dfe);
  *sel_place = save_sel;
  return dfe;
}
