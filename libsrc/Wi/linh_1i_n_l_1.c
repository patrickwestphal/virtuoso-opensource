



{
  if (2 & CONC (stat_, 1))
    {
      RESULT (1);
    }
  else if (1 == CONC (stat_, 1))
    {
      for (inx = 0; inx < 7; inx++)
	{
	  INC_LOW_3 (CONC (pos1_, 1));
	  CONC (w_, 1) = CONC (array_, 1)[CONC (pos1_, 1)];
	  if (CHA_EMPTY == CONC (w_, 1))
	    {
	      CH_PRE (1, 1 + BATCH, next_stat_);
	      goto CONC (done_, 1);
	    }
	  if (CONC (w_, 1) == values[SET_NO (1)])
	    {
	      RESULT (1);
	      CH_PRE (1, 1 + BATCH, next_stat_);
	      goto CONC (done_, 1);
	    }
	}
      EXCEPT (1);
    }
CONC (done_, 1):;
  CH_PRE (1, 1 + BATCH, next_stat_);
}


#undef done_1
#undef e_done_1
