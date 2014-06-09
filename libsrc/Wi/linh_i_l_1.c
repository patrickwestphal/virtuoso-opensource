

/* stat_1 == 3 means there is a match of high 16 in the bucket
   stat_1 == 1 means there is non null data in the bucket.
   stat_1 == 4 means must look at exceptions
   stat_1 == 9 means the bucket is to be skipped, already done  or not in scope
*/




if (stat_1 != 9)
  {
    int inx;
#ifdef IGN_IF
    IGN_IF (1);
#endif

    for (inx = 0; inx < 8; inx++)
      {
	w_1 = array_1[pos1_1];
	if (0 == w_1)
	  {
	    L_INSERT (1);
	    goto done_1;
	  }
	if (HIGH_16 (CONC (w_, 1)) == CONC (hash_, 1))
	  {
	    if (HIT_N (1, w_1))
	      {
		RESULT (1);
		goto done_1;
	      }
	  }
	INC_LOW_3 (CONC (pos1_, 1));
      }
    for (inx = 0; inx < cha_p_1->cha_exception_fill; inx++)
      {
	w_1 = (int64) cha_p_1->cha_exceptions[inx];
	if (HIGH_16 (w_1) != hash_1)
	  continue;
	w_1 = LOW_48 (w_1);
	if (HIT_N_E (1, w_1))
	  {
	    RESULT (1);
	    goto done_1;
	  }
      }
    L_E_INSERT (1);
  done_1:
    CH_PRE (1, BATCH + 1, stat_);
  }
