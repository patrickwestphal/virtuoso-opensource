

/* stat_NN == 3 means there is a match of high 16 in the bucket
   stat_NN == 1 means there is non null data in the bucket.
   stat_NN == 9 means the bucket is to be skipped, already done  or not in scope
*/




{
  if (3 == CONC (stat_, NN))
    {
      CH_PREFETCH (NN);
    }
  else if (1 == CONC (stat_, NN))
    {
      for (;;)
	{
	  if (!--to_go_NN)
	    {
	      stat_NN = 4;
	      goto done_NN;
	    }
	  INC_LOW_3 (pos1_NN);
	  w_NN = array_NN[pos1_NN];
	  if (0 == w_NN)
	    goto miss_NN;
	  if (HIGH_16 (CONC (w_, NN)) == CONC (hash_, NN))
	    {
	      CH_PREFETCH (NN);
	      stat_NN = 3;
	      goto done_NN;
	    }
	}
    }
  else if (CONC (stat_, NN) != 9)
    {
    miss_NN:
      CH_PRE (NN, NN + BATCH, next_stat_);
      stat_NN = 9;
    }
CONC (done_, NN):;
}

#undef miss_NN
#undef done_NN
