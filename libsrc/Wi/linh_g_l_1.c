

/* stat_1 == 3 means there is a match of high 16 in the bucket
   stat_1 == 1 means there is non null data in the bucket.
   stat_1 == 9 means the bucket is to be skipped, already done  or not in scope
*/




{
#ifdef LINH_TRAP
  if (9 != stat_1)
    {
      LINH_TRAP (1);
    }
#endif
  if (3 == CONC (stat_, 1))
    {
      CH_PREFETCH (1);
    }
  else if (1 == CONC (stat_, 1))
    {
      for (;;)
	{
	  if (!--to_go_1)
	    {
	      stat_1 = 4;
	      goto done_1;
	    }
	  INC_LOW_3 (pos1_1);
	  w_1 = array_1[pos1_1];
	  if (0 == w_1)
	    goto miss_1;
	  if (HIGH_16 (CONC (w_, 1)) == CONC (hash_, 1))
	    {
	      CH_PREFETCH (1);
	      stat_1 = 3;
	      goto done_1;
	    }
	}
    }
  else if (CONC (stat_, 1) != 9)
    {
    miss_1:
      CH_PRE (1, 1 + BATCH, next_stat_);
      stat_1 = 9;
    }
CONC (done_, 1):;
}

#undef miss_1
#undef done_1
