



{
  if (2 & CONC (stat_, 2))
    {
      RESULT (2);
    }
  else if (1 == CONC (stat_, 2))
    {
      for (inx = 0; inx < 7; inx++)
	{
	  INC_LOW_3 (CONC (pos1_, 2));
	  CONC (w_, 2) = CONC (array_, 2)[CONC (pos1_, 2)];
	  if (CHA_EMPTY == CONC (w_, 2))
	    {
	      CH_PRE (2, 2 + BATCH, next_stat_);
	      goto CONC (done_, 2);
	    }
	  if (CONC (w_, 2) == values[SET_NO (2)])
	    {
	      RESULT (2);
	      CH_PRE (2, 2 + BATCH, next_stat_);
	      goto CONC (done_, 2);
	    }
	}
      EXCEPT (2);
    }
CONC (done_, 2):;
  CH_PRE (2, 2 + BATCH, next_stat_);
}


#undef done_2
#undef e_done_2
