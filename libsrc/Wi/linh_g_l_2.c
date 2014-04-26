

/* stat_2 == 3 means there is a match of high 16 in the bucket
   stat_2 == 1 means there is non null data in the bucket.
   stat_2 == 9 means the bucket is to be skipped, already done  or not in scope
*/




{
  if (3 == CONC (stat_, 2))
    {
      CH_PREFETCH (2);
    }
  else if (1 == CONC (stat_, 2))
    {
      for (;;)
	{
	  if (!--to_go_2)
	    {
	      stat_2 = 4;
	      goto done_2;
	    }
	  INC_LOW_3 (pos1_2);
	  w_2 = array_2[pos1_2];
	  if (0 == w_2)
	    goto miss_2;
	  if (HIGH_16 (CONC (w_, 2)) == CONC (hash_, 2))
	    {
	      CH_PREFETCH (2);
	      stat_2 = 3;
	      goto done_2;
	    }
	}
    }
  else if (CONC (stat_, 2) != 9)
    {
    miss_2:
      CH_PRE (2, 2 + BATCH, next_stat_);
      stat_2 = 9;
    }
CONC (done_, 2):;
}

#undef miss_2
#undef done_2
