


if (3 == stat_0)
  {
    for (;;)
      {
	w_0 = LOW_48 (w_0);
	if (HIT_N (0, w_0))
	  {
	    RESULT (0);
	    goto done_c_0;
	  }
      next_0:
	if (!--to_go_0)
	  break;
	INC_LOW_3 (pos1_0);
	w_0 = array_0[pos1_0];
	if (0 == w_0)
	  goto done_c_0;
	if (HIGH_16 (w_0) != hash_0)
	  goto next_0;
      }
  exc_0:
    EXCEPT (0);
  done_c_0:
    CH_PRE (0, BATCH + 0, next_stat_);
  }
else if (4 == stat_0)
  goto exc_0;

#undef done_c_0
#undef next_0
#undef exc_0
