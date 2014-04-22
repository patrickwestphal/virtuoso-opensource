


/* linear hash template for single int hash table */


#define BATCH 4


#define VARS(n)					\
  chash_t * cha_p_##n; \
  int64 * array_##n; \
  uint64 hash_##n; \
  int64 w_##n; \
  int set_##n, pos1_##n; \
  char stat_##n = 8, next_stat_##n = 0;

#define L_SET_ANY(stat, bool) stat |= bool
#define L_SET_CAND(stat, bool) stat |= (bool) << 1



#define CH_PRE(var_n, read_n, stat)		\
if (set + read_n < LAST) \
  { \
    stat##var_n = 0;			       \
    set_##var_n = SET_NO_I (set + read_n); \
    HASH (var_n); \
    cha_p_##var_n = CHA_PARTITION (cha, hash_##var_n); \
    array_##var_n = (int64*)CHA_ARRAY (cha, cha_p_##var_n, hash_##var_n); \
    pos1_##var_n = CHA_LIN_POS (cha, hash_##var_n); \
    w_##var_n = array_##var_n[pos1_##var_n]; \
    if (CHA_EMPTY == values[SET_NO (var_n)]) \
      stat##var_n = 4;		      \
    else			      \
      {							     \
	L_SET_ANY (stat##var_n, CHA_EMPTY != w_##var_n);	     \
	L_SET_CAND (stat##var_n, w_##var_n == values[SET_NO (var_n)]);	\
      }							     \
  } \
 else  \
  stat##var_n = 0  \


#define EXCEPT(n) \
    { int inx;							\
  for (inx = 0; inx < cha_p_##n->cha_exception_fill; inx++)	\
    {								\
      if (values[SET_NO (n)] == (int64)cha_p_##n->cha_exceptions[inx])	\
    {\
  RESULT_E (n);\
  goto e_done_##n;						\
}								\
    }\
e_done_##n: ; \
}


#define NEXT(n) \
  {stat_##n = next_stat_##n; next_stat_##n = 0;}


for (set = FIRST; set < LAST; set += BATCH)
  {
    VARS (0);
    VARS (1);
    VARS (2);
    VARS (3);
    CH_PRE (0, 0, stat_);
    CH_PRE (1, 1, stat_);
    CH_PRE (2, 2, stat_);
    CH_PRE (3, 3, stat_);

    for (set = FIRST; set < LAST; set += BATCH)
      {

#include "linh_1i_n_l_0.c"
#include "linh_1i_n_l_1.c"
#include "linh_1i_n_l_2.c"
#include "linh_1i_n_l_3.c"

	NEXT (0);
	NEXT (1);
	NEXT (2);
	NEXT (3);
      }
  }


#undef LAST
#undef FIRST
#undef SET_NO
#undef SET_NO_I
#undef HASH
#undef RESULT
#undef RESULT_E
#undef LOOK
#undef EXCEPT
#undef VARS
#undef CH_PRE
#undef BATCH
#undef NEXT
