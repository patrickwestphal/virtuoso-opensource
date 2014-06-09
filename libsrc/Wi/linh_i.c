


/* linear hash template for general hash table with insert */


#define BATCH 4


#define VARS(n)					\
  chash_t * cha_p_##n; \
  int64 * array_##n; \
  uint64 hash_##n; \
  int64 w_##n; \
  int set_##n, pos1_##n; \
  char stat_##n;



#define CH_PRE(var_n, read_n, stat)		\
if (set + read_n < LAST) \
  { \
    stat##var_n = 0;			       \
    set_##var_n = SET_NO_I (set + read_n); \
    HASH (var_n);				       \
    cha_p_##var_n = CHA_PARTITION (cha, hash_##var_n); \
    array_##var_n = (int64*)CHA_ARRAY (cha, cha_p_##var_n, hash_##var_n); \
    pos1_##var_n = CHA_LIN_POS (cha, hash_##var_n); \
    hash_##var_n = HIGH_16 (hash_##var_n); \
    __builtin_prefetch (&array_##var_n[pos1_##var_n]); \
  } \
  else						\
    stat##var_n = 9				\





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
#include "linh_i_l_0.c"
#include "linh_i_l_1.c"
#include "linh_i_l_2.c"
#include "linh_i_l_3.c"
      }
  }


#undef LAST
#undef FIRST
#undef SET_NO
#undef SET_NO_I
#undef RESULT
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
#undef IGN_IF
