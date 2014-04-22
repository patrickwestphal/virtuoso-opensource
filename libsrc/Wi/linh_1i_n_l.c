



{
  if (2 & CONC (stat_, NN))
    {
      RESULT (NN);
    }
  else if (1 == CONC (stat_, NN))
    {
      for (inx = 0; inx < 7; inx++)
	{
	  INC_LOW_3 (CONC (pos1_, NN));
	  CONC (w_, NN) = CONC (array_, NN)[CONC (pos1_, NN)];
	  if (CHA_EMPTY == CONC (w_, NN))
	    {
	      CH_PRE (NN, NN + BATCH, next_stat_);
	      goto CONC (done_, NN);
	    }
	  if (CONC (w_, NN) == values[SET_NO (NN)])
	    {
	      RESULT (NN);
	      CH_PRE (NN, NN + BATCH, next_stat_);
	      goto CONC (done_, NN);
	    }
	}
      EXCEPT (NN);
    }
CONC (done_, NN):;
  CH_PRE (NN, NN + BATCH, next_stat_);
}


#undef done_NN
#undef e_done_NN
