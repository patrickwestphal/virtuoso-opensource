

/* stat_3 == 3 means there is a match of high 16 in the bucket
   stat_3 == 1 means there is non null data in the bucket.
   stat_3 == 9 means the bucket is to be skipped, already done  or not in scope
*/




{
#ifdef LINH_TRAP
  if (9 != stat_3)
    {
      LINH_TRAP (3);
    }
#endif
  if (3 == CONC (stat_, 3))
    {
      CH_PREFETCH (3);
    }
  else if (1 == CONC (stat_, 3))
    {
      for (;;)
	{
	  if (!--to_go_3)
	    {
	      stat_3 = 4;
	      goto done_3;
	    }
	  INC_LOW_3 (pos1_3);
	  w_3 = array_3[pos1_3];
	  if (0 == w_3)
	    goto miss_3;
	  if (HIGH_16 (CONC (w_, 3)) == CONC (hash_, 3))
	    {
	      CH_PREFETCH (3);
	      stat_3 = 3;
	      goto done_3;
	    }
	}
    }
  else if (CONC (stat_, 3) != 9)
    {
    miss_3:
      CH_PRE (3, 3 + BATCH, next_stat_);
      stat_3 = 9;
    }
CONC (done_, 3):;
}

#undef miss_3
#undef done_3
