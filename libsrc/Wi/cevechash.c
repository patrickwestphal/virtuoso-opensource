/*
 *  cevechash.c  - Template for inlined hash join for int array ce's
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



int
CHA_1I_N_NAME (hash_source_t * hs, chash_t * cha, it_cursor_t * itc, row_no_t * matches, int n_matches, int match_out, int ce_row,
    ELT_T * values)
{
  int set, inx;


#define SET_NO(n)  set_##n
#define SET_NO_I(i) matches[i]
#define HASH(n) \
  MHASH_STEP_1 (hash_##n, values[set_##n])

#define FIRST match_out
#define LAST n_matches

#define RESULT(n) \
  matches[match_out++] = set_##n;
#define RESULT_E(n) RESULT (n)

#include "linh_1i_n.c"
  return match_out;
}

int
CHA_1I_NAME (hash_source_t * hs, chash_t * cha, it_cursor_t * itc, row_no_t * matches, int n_matches, int match_out, int ce_row,
    ELT_T * values)
{
  int set;
  int n_res = hs ? hs->hs_ha->ha_n_deps : 0;
  caddr_t *inst = itc->itc_out_state;

#undef HIT_N
#undef HIT_N_E
#define HASH(n) \
  MHASH_STEP_1 (hash_##n, values[set_##n])


#define HIT_N(n, e)   (((int64*)e)[0] == values[set_##n])
#define HIT_N_E(n, e) HIT_N(n, e)
#define FIRST match_out
#define LAST n_matches
#define SET_NO_I(i) matches[i]
#define SET_NO(n) set_##n
#define CH_PREFETCH(n) __builtin_prefetch ((void*) LOW_48 (w_##n))

#define RESULT(n) \
  {						       \
    matches[match_out++] = set_##n;				\
    if (n_res) cha_inline_result (hs, cha, inst, (int64*)LOW_48 ((int64)w_##n), 1); \
  }
#define RESULT_E(n) RESULT (n)

#include "linh_g.c"
  return match_out;
}



#define CHB_VAR(n)		  \
  uint64 h##n, w##n, mask##n;

#define CHB_INIT(n, i) \
  MHASH_STEP_1 (h##n, i); \
  w##n = bf[BF_WORD (h##n, sz)]; \
  mask##n = BF_MASK (h##n);


#define CHB_CK(n) \
  {matches[mfill] = inx + n;			\
   mfill += (w##n & mask##n) == mask##n;}

#define CHB_CK_S(n, row)				\
  {matches[mfill] = row;				\
   mfill += (w##n & mask##n) == mask##n;}


int
RANGE_NAME (col_pos_t * cpo, db_buf_t ce_first, int n_values, int n_bytes)
{
  it_cursor_t *itc = cpo->cpo_itc;
  search_spec_t *sp = itc->itc_col_spec;
  hash_range_spec_t *hrng = (hash_range_spec_t *) sp->sp_min_ssl;
  caddr_t *inst = itc->itc_out_state;
  int ce_row = cpo->cpo_ce_row_no;
  chash_t *cha = cpo->cpo_chash;
  uint64 *bf = cha->cha_bloom;
  uint32 sz = cha->cha_n_bloom;
  row_no_t *matches = itc->itc_matches;
  int mfill = itc->itc_match_out, inx;
  int init_matches = mfill;
  int last = MIN (ce_row + n_values, cpo->cpo_to);
  uint32 min = QST_INT (inst, hrng->hrng_min);
  uint32 max = QST_INT (inst, hrng->hrng_max);
  if (EXPECT_DV != cha->cha_sqt[0].sqt_dtp || !enable_int_vec_hash)
    return ce_hash_range_filter (cpo, ce_first, n_values, n_bytes);
  ce_first -= sizeof (ELT_T) * ce_row;
  if (hrng->hrng_min && (min != 0 || max != 0xffffffff))
    {
      for (inx = ce_row + cpo->cpo_skip; inx < last; inx++)
	{
	  uint64 h, w, mask;
	  MHASH_STEP_1 (h, REF ((ce_first + sizeof (ELT_T) * inx)));
	  if (H_PART (h) >= min && H_PART (h) <= max)
	    {
	      if (bf)
		{
		  w = bf[BF_WORD (bf, sz)];
		  mask = BF_MASK (h);
		  matches[mfill] = inx;
		  mfill += mask == (w & mask);
		}
	      else
		matches[mfill++] = inx;
	    }
	}
    }
  else if (bf)
    {
      for (inx = ce_row + cpo->cpo_skip; inx + 4 <= last; inx += 4)
	{
	  CHB_VAR (0);
	  CHB_VAR (1);
	  CHB_VAR (2);
	  CHB_VAR (3);
	  CHB_INIT (0, REF ((ce_first + sizeof (ELT_T) * inx)));
	  CHB_INIT (1, REF ((ce_first + sizeof (ELT_T) * (inx + 1))));
	  CHB_INIT (2, REF ((ce_first + sizeof (ELT_T) * (inx + 2))));
	  CHB_INIT (3, REF ((ce_first + sizeof (ELT_T) * (inx + 3))));
	  CHB_CK (0);
	  CHB_CK (1);
	  CHB_CK (2);
	  CHB_CK (3);
	}
      for (inx = inx; inx < last; inx++)
	{
	  CHB_VAR (0);
	  CHB_INIT (0, REF ((ce_first + sizeof (ELT_T) * inx)));
	  CHB_CK (0);
	}
    }
  else
    {
      int r;
      /* no hash no rage and no bloom, all rows in range in the ce are checked by the hash lookup */
      for (r = ce_row + cpo->cpo_skip; r < last; r++)
	matches[mfill++] = r;
    }
  if (hrng->hrng_hs)
    {
      if (cha->cha_is_1_int)
	mfill = CHA_1I_N_NAME (hrng->hrng_hs, cha, itc, matches, mfill, init_matches, ce_row, (ELT_T *) ce_first);
      else
	mfill = CHA_1I_NAME (hrng->hrng_hs, cha, itc, matches, mfill, init_matches, ce_row, (ELT_T *) ce_first);
    }
  itc->itc_match_out = mfill;
  return ce_row + n_values;
}


int
SETS_NAME (col_pos_t * cpo, db_buf_t ce_first, int n_values, int n_bytes)
{
  it_cursor_t *itc = cpo->cpo_itc;
  int s1, s2, s3, s4;
  search_spec_t *sp = itc->itc_col_spec;
  hash_range_spec_t *hrng = (hash_range_spec_t *) sp->sp_min_ssl;
  caddr_t *inst = itc->itc_out_state;
  int ce_row = cpo->cpo_ce_row_no;
  chash_t *cha = cpo->cpo_chash;
  uint64 *bf = cha->cha_bloom;
  uint32 sz = cha->cha_n_bloom;
  row_no_t *matches = itc->itc_matches;
  int mfill = itc->itc_match_out, inx = itc->itc_match_in;
  int init_matches = mfill;
  int n_matches = itc->itc_n_matches;
  int last = ce_row + n_values;
  uint32 min = QST_INT (inst, hrng->hrng_min);
  uint32 max = QST_INT (inst, hrng->hrng_max);
  if (EXPECT_DV != cha->cha_sqt[0].sqt_dtp || !enable_int_vec_hash)
    return ce_hash_sets_filter (cpo, ce_first, n_values, n_bytes);
  ce_first -= sizeof (ELT_T) * ce_row;
  if (hrng->hrng_min && (min != 0 || max != 0xffffffff))
    {
      while (inx < n_matches && (s1 = matches[inx]) < last)
	{
	  uint64 h, w, mask;
	  MHASH_STEP_1 (h, REF ((ce_first + sizeof (ELT_T) * s1)));
	  if (H_PART (h) >= min && H_PART (h) <= max)
	    {
	      if (bf)
		{
		  w = bf[BF_WORD (bf, sz)];
		  mask = BF_MASK (h);
		  matches[mfill] = s1;
		  mfill += mask == (w & mask);
		}
	      else
		matches[mfill++] = s1;
	    }
	  inx++;
	}
    }
  else if (bf)
    {
      while (inx + 4 <= n_matches && (s4 = matches[inx + 3]) < last)
	{
	  CHB_VAR (0);
	  CHB_VAR (1);
	  CHB_VAR (2);
	  CHB_VAR (3);
	  s1 = matches[inx];
	  s2 = matches[inx + 1];
	  s3 = matches[inx + 2];
	  CHB_INIT (0, REF ((ce_first + sizeof (ELT_T) * s1)));
	  CHB_INIT (1, REF ((ce_first + sizeof (ELT_T) * s2)));
	  CHB_INIT (2, REF ((ce_first + sizeof (ELT_T) * s3)));
	  CHB_INIT (3, REF ((ce_first + sizeof (ELT_T) * s4)));
	  CHB_CK_S (0, s1);
	  CHB_CK_S (1, s2);
	  CHB_CK_S (2, s3);
	  CHB_CK_S (3, s4);
	  inx += 4;
	}
      while (inx < n_matches && (s1 = matches[inx]) < last)
	{
	  CHB_VAR (0);
	  CHB_INIT (0, REF ((ce_first + sizeof (ELT_T) * s1)));
	  CHB_CK_S (0, s1);
	  inx++;
	}
    }
  else
    {
      while (inx < n_matches && (s1 = matches[inx]) < last)
	{
	  mfill++;
	  inx++;
	}
    }
  if (hrng->hrng_hs)
    {
      if (cha->cha_is_1_int)
	mfill = CHA_1I_N_NAME (hrng->hrng_hs, cha, itc, matches, mfill, init_matches, ce_row, (ELT_T *) ce_first);
      else
	mfill = CHA_1I_NAME (hrng->hrng_hs, cha, itc, matches, mfill, init_matches, ce_row, (ELT_T *) ce_first);
    }
  itc->itc_match_in = inx;
  itc->itc_match_out = mfill;
  return itc->itc_match_in >= itc->itc_n_matches ? CE_AT_END : itc->itc_matches[itc->itc_match_in];
}

#undef RANGE_NAME
#undef SETS_NAME
#undef ELT_T
#undef ELT_T
#undef REF
#undef DTP_MIN
#undef DTP_MAX
#undef DTP
#undef CHB_VAR
#undef CHA_1I_NAME
#undef CHA_1I_N_NAME
#undef CHB_INIT
#undef CHB_CK
#undef CHB_CK_S
#undef EXPECT_DV
