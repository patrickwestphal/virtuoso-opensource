

/* stat_0 == 3 means there is a match of high 16 in the bucket
   stat_0 == 1 means there is non null data in the bucket.
   stat_0 == 4 means must look at exceptions
   stat_0 == 9 means the bucket is to be skipped, already done  or not in scope
*/




if (stat_0 != 9)
  {
    int inx;
#ifdef IGN_IF
    IGN_IF (0);
#endif

    for (inx = 0; inx < 8; inx++)
      {
	w_0 = array_0[pos1_0];
	if (0 == w_0)
	  {
	    L_INSERT (0);
	    goto done_0;
	  }
	if (HIGH_16 (CONC (w_, 0)) == CONC (hash_, 0))
	  {
	    if (HIT_N (0, w_0))
	      {
		RESULT (0);
		goto done_0;
	      }
	  }
	INC_LOW_3 (CONC (pos1_, 0));
      }
    for (inx = 0; inx < cha_p_0->cha_exception_fill; inx++)
      {
	w_0 = (int64) cha_p_0->cha_exceptions[inx];
	if (HIGH_16 (w_0) != hash_0)
	  continue;
	w_0 = LOW_48 (w_0);
	if (HIT_N_E (0, w_0))
	  {
	    RESULT (0);
	    goto done_0;
	  }
      }
    L_E_INSERT (0);
  done_0:
    CH_PRE (0, BATCH + 0, stat_);
  }
