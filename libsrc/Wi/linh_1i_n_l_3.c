



{
  if (2 & CONC (stat_, 3))
    {
      RESULT (3);
    }
  else if (1 == CONC (stat_, 3))
    {
      for (inx = 0; inx < 7; inx++)
	{
	  INC_LOW_3 (CONC (pos1_, 3));
	  CONC (w_, 3) = CONC (array_, 3)[CONC (pos1_, 3)];
	  if (CHA_EMPTY == CONC (w_, 3))
	    {
	      CH_PRE (3, 3 + BATCH, next_stat_);
	      goto CONC (done_, 3);
	    }
	  if (CONC (w_, 3) == values[SET_NO (3)])
	    {
	      RESULT (3);
	      CH_PRE (3, 3 + BATCH, next_stat_);
	      goto CONC (done_, 3);
	    }
	}
      EXCEPT (3);
    }
CONC (done_, 3):;
  CH_PRE (3, 3 + BATCH, next_stat_);
}


#undef done_3
#undef e_done_3
