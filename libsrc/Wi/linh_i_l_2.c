

/* stat_2 == 3 means there is a match of high 16 in the bucket
   stat_2 == 1 means there is non null data in the bucket.
   stat_2 == 4 means must look at exceptions
   stat_2 == 9 means the bucket is to be skipped, already done  or not in scope
*/




if (stat_2 != 9)
  {
    int inx;
    for (inx = 0; inx < 8; inx++)
      {
	w_2 = array_2[pos1_2];
	if (0 == w_2)
	  {
	    L_INSERT (2);
	    goto done_2;
	  }
	if (HIGH_16 (CONC (w_, 2)) == CONC (hash_, 2))
	  {
	    if (HIT_N (2, w_2))
	      {
		RESULT (2);
		goto done_2;
	      }
	  }
	INC_LOW_3 (CONC (pos1_, 2));
      }
    for (inx = 0; inx < cha_p_2->cha_exception_fill; inx++)
      {
	w_2 = (int64) cha_p_2->cha_exceptions[inx];
	if (HIGH_16 (w_2) != hash_2)
	  continue;
	w_2 = LOW_48 (w_2);
	if (HIT_N_E (2, w_2))
	  {
	    RESULT (2);
	    goto done_2;
	  }
      }
    L_E_INSERT (2);
  done_2:
    CH_PRE (2, BATCH + 2, stat_);
  }
