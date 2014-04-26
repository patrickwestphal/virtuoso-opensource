


if (3 == stat_3)
  {
    for (;;)
      {
	w_3 = LOW_48 (w_3);
	if (HIT_N (3, w_3))
	  {
	    RESULT (3);
	    goto done_c_3;
	  }
      next_3:
	if (!--to_go_3)
	  break;
	INC_LOW_3 (pos1_3);
	w_3 = array_3[pos1_3];
	if (0 == w_3)
	  goto done_c_3;
	if (HIGH_16 (w_3) != hash_3)
	  goto next_3;
      }
  exc_3:
    EXCEPT (3);
  done_c_3:
    CH_PRE (3, BATCH + 3, next_stat_);
  }
else if (4 == stat_3)
  goto exc_3;

#undef done_c_3
#undef next_3
#undef exc_3
