


if (3 == stat_1)
  {
    for (;;)
      {
	w_1 = LOW_48 (w_1);
	if (HIT_N (1, w_1))
	  {
	    RESULT (1);
	    goto done_c_1;
	  }
      next_1:
	if (!--to_go_1)
	  break;
	INC_LOW_3 (pos1_1);
	w_1 = array_1[pos1_1];
	if (0 == w_1)
	  goto done_c_1;
	if (HIGH_16 (w_1) != hash_1)
	  goto next_1;
      }
  exc_1:
    EXCEPT (1);
  done_c_1:
    CH_PRE (1, BATCH + 1, next_stat_);
  }
else if (4 == stat_1)
  goto exc_1;

#undef done_c_1
#undef next_1
#undef exc_1
