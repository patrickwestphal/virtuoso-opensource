


/* linear hash template for general hash table */


#define BATCH 4


#define VARS(n)					\
  chash_t * cha_p_##n; \
  int64 * array_##n; \
  uint64 hash_##n; \
  int64 w_##n; \
  int set_##n, pos1_##n; \
  char stat_##n, next_stat_##n = 0, to_go_##n;

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
    hash_##var_n = HIGH_16 (hash_##var_n); \
    w_##var_n = array_##var_n[pos1_##var_n]; \
    L_SET_ANY (stat##var_n, 0 != w_##var_n);			\
L_SET_CAND (stat##var_n, HIGH_16 (w_##var_n) == hash_##var_n);		\
    to_go_##var_n = 8; \
  } \
 else						\
   stat##var_n = 9				\



#define EXCEPT(n) \
    { int inx;							\
  for (inx = 0; inx < cha_p_##n->cha_exception_fill; inx++)	\
    {								\
      w_##n = (int64)cha_p_##n->cha_exceptions[inx];	\
      if (HIGH_16 (w_##n) != hash_##n) \
	continue; \
      w_##n = LOW_48 (w_##n);		\
      if (HIT_N_E (n, w_##n))		\
    {\
  RESULT_E (n);\
  break;						\
}								\
    } \
  stat_##n = 9;					\
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
#include "linh_g_l_0.c"
#include "linh_g_l_1.c"
#include "linh_g_l_2.c"
#include "linh_g_l_3.c"

#include "linh_g_c_0.c"
#include "linh_g_c_1.c"
#include "linh_g_c_2.c"
#include "linh_g_c_3.c"




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
#undef RESULT
#undef RESULT_E
#undef LOOK
#undef EXCEPT
#undef VARS
#undef CH_PRE
#undef HASH
#undef BATCH
#undef NEXT
#undef CH_PREFETCH
#undef L_E_INSERT
#undef L_INSERT
