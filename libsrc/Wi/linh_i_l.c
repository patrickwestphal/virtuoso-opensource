

/* stat_NN == 3 means there is a match of high 16 in the bucket
   stat_NN == 1 means there is non null data in the bucket.
   stat_NN == 4 means must look at exceptions
   stat_NN == 9 means the bucket is to be skipped, already done  or not in scope
*/




if (stat_NN != 9)
  {
    int inx;
#ifdef IGN_IF
    IGN_IF (NN);
#endif

    for (inx = 0; inx < 8; inx++)
      {
	w_NN = array_NN[pos1_NN];
	if (0 == w_NN)
	  {
	    L_INSERT (NN);
	    goto done_NN;
	  }
	if (HIGH_16 (CONC (w_, NN)) == CONC (hash_, NN))
	  {
	    if (HIT_N (NN, w_NN))
	      {
		RESULT (NN);
		goto done_NN;
	      }
	  }
	INC_LOW_3 (CONC (pos1_, NN));
      }
    for (inx = 0; inx < cha_p_NN->cha_exception_fill; inx++)
      {
	w_NN = (int64) cha_p_NN->cha_exceptions[inx];
	if (HIGH_16 (w_NN) != hash_NN)
	  continue;
	w_NN = LOW_48 (w_NN);
	if (HIT_N_E (NN, w_NN))
	  {
	    RESULT (NN);
	    goto done_NN;
	  }
      }
    L_E_INSERT (NN);
  done_NN:
    CH_PRE (NN, BATCH + NN, stat_);
  }
