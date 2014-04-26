



{
  if (2 & CONC (stat_, 0))
    {
      RESULT (0);
    }
  else if (1 == CONC (stat_, 0))
    {
      for (inx = 0; inx < 7; inx++)
	{
	  INC_LOW_3 (CONC (pos1_, 0));
	  CONC (w_, 0) = CONC (array_, 0)[CONC (pos1_, 0)];
	  if (CHA_EMPTY == CONC (w_, 0))
	    {
	      CH_PRE (0, 0 + BATCH, next_stat_);
	      goto CONC (done_, 0);
	    }
	  if (CONC (w_, 0) == values[SET_NO (0)])
	    {
	      RESULT (0);
	      CH_PRE (0, 0 + BATCH, next_stat_);
	      goto CONC (done_, 0);
	    }
	}
      EXCEPT (0);
    }
CONC (done_, 0):;
  CH_PRE (0, 0 + BATCH, next_stat_);
}


#undef done_0
#undef e_done_0
