

/* stat_0 == 3 means there is a match of high 16 in the bucket
   stat_0 == 1 means there is non null data in the bucket.
   stat_0 == 9 means the bucket is to be skipped, already done  or not in scope
*/




{
  if (3 == CONC (stat_, 0))
    {
      CH_PREFETCH (0);
    }
  else if (1 == CONC (stat_, 0))
    {
      for (;;)
	{
	  if (!--to_go_0)
	    {
	      stat_0 = 4;
	      goto done_0;
	    }
	  INC_LOW_3 (pos1_0);
	  w_0 = array_0[pos1_0];
	  if (0 == w_0)
	    goto miss_0;
	  if (HIGH_16 (CONC (w_, 0)) == CONC (hash_, 0))
	    {
	      CH_PREFETCH (0);
	      stat_0 = 3;
	      goto done_0;
	    }
	}
    }
  else if (CONC (stat_, 0) != 9)
    {
    miss_0:
      CH_PRE (0, 0 + BATCH, next_stat_);
      stat_0 = 9;
    }
CONC (done_, 0):;
}

#undef miss_0
#undef done_0
