


if (3 == stat_2)
  {
    for (;;)
      {
	w_2 = LOW_48 (w_2);
	if (HIT_N (2, w_2))
	  {
	    RESULT (2);
	    goto done_c_2;
	  }
      next_2:
	if (!--to_go_2)
	  break;
	INC_LOW_3 (pos1_2);
	w_2 = array_2[pos1_2];
	if (0 == w_2)
	  goto done_c_2;
	if (HIGH_16 (w_2) != hash_2)
	  goto next_2;
      }
  exc_2:
    EXCEPT (2);
  done_c_2:
    CH_PRE (2, BATCH + 2, next_stat_);
  }
else if (4 == stat_2)
  goto exc_2;

#undef done_c_2
#undef next_2
#undef exc_2
