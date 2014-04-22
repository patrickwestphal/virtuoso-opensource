


if (3 == stat_NN)
  {
    for (;;)
      {
	w_NN = LOW_48 (w_NN);
	if (HIT_N (NN, w_NN))
	  {
	    RESULT (NN);
	    goto done_c_NN;
	  }
      next_NN:
	if (!--to_go_NN)
	  break;
	INC_LOW_3 (pos1_NN);
	w_NN = array_NN[pos1_NN];
	if (0 == w_NN)
	  goto done_c_NN;
	if (HIGH_16 (w_NN) != hash_NN)
	  goto next_NN;
      }
  exc_NN:
    EXCEPT (NN);
  done_c_NN:
    CH_PRE (NN, BATCH + NN, next_stat_);
  }
else if (4 == stat_NN)
  goto exc_NN;

#undef done_c_NN
#undef next_NN
#undef exc_NN
