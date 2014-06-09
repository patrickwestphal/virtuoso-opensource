

/* stat_3 == 3 means there is a match of high 16 in the bucket
   stat_3 == 1 means there is non null data in the bucket.
   stat_3 == 4 means must look at exceptions
   stat_3 == 9 means the bucket is to be skipped, already done  or not in scope
*/




if (stat_3 != 9)
  {
    int inx;
#ifdef IGN_IF
    IGN_IF (3);
#endif

    for (inx = 0; inx < 8; inx++)
      {
	w_3 = array_3[pos1_3];
	if (0 == w_3)
	  {
	    L_INSERT (3);
	    goto done_3;
	  }
	if (HIGH_16 (CONC (w_, 3)) == CONC (hash_, 3))
	  {
	    if (HIT_N (3, w_3))
	      {
		RESULT (3);
		goto done_3;
	      }
	  }
	INC_LOW_3 (CONC (pos1_, 3));
      }
    for (inx = 0; inx < cha_p_3->cha_exception_fill; inx++)
      {
	w_3 = (int64) cha_p_3->cha_exceptions[inx];
	if (HIGH_16 (w_3) != hash_3)
	  continue;
	w_3 = LOW_48 (w_3);
	if (HIT_N_E (3, w_3))
	  {
	    RESULT (3);
	    goto done_3;
	  }
      }
    L_E_INSERT (3);
  done_3:
    CH_PRE (3, BATCH + 3, stat_);
  }
