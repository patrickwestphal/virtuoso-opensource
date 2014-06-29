


#define MAX_BIT 2000


/* bits_inst_t describes an instance of a bits index extension.  It stores all information pertaining to the instance. */

typedef struct bits_inst_s
{
  dk_hash_t *bi_id_to_int;
  dk_hash_t *bi_int_to_id;
  int64 bi_last_int;
  char **bi_vecs;
  char *bi_file;
  rwlock_t *bi_rw;
} bits_inst_t;


typedef struct bits_cr_s
{
  bits_inst_t *bcr_bi;
  int *bcr_bits;
  int bcr_n_bits;
  int bcr_min_bits;
  char bcr_mode;
  int64 bcr_pos;
  int64 bcr_word;
} bits_cr_t;


typedef struct bits_txn_s
{
  bits_inst_t *btx_bi;
  dk_hash_t *btx_del_at_rb;
  dk_hash_t *btx_del_at_commit;
} bits_txn_t;

/*bcr_pos*/
#define BCR_INIT -1L
#define BCR_AT_END -2L

/*bcr_mode*/
#define BCR_AND 0
#define BCR_OR 1

#define is_digit(c) ((c) >= '0'&& (c) <= '9')
int
bits_read_line (dk_session_t * ses, char *buf, int max)
{
  int inx = 0;
  buf[0] = 0;

  for (;;)
    {
      char c = session_buffered_read_char (ses);
      if (inx < max - 1)
	buf[inx++] = c;
      if (c == 10)
	{
	  buf[inx] = 0;
	  return inx - 1;
	}
    }
}



int *
bits_parse (char *str)
{
  int *bits;
  int res[MAX_BIT];
  int i, n = -1, fill = 0;
  for (i = 0; str[i]; i++)
    {
      char c = str[i];
      if (!is_digit (c))
	{
	  if (-1 != n)
	    {
	      res[fill++] = n;
	      n = -1;
	      if (fill == MAX_BIT - 1)
		break;
	    }
	}
      else
	{
	  if (-1 == n)
	    n = c - '0';
	  else
	    n *= c - '0';
	}
    }
  if (-1 != n)
    {
      res[fill++] = n;
    }
  bits = dk_alloc_box (fill * sizeof (int), DV_BIN);
  memcpy (bits, res, fill * sizeof (int));
  return bits;
}

void
bits_set_bit (bits_inst_t * bi, int64 id, int bit)
{
  int len;
  char *vec;
  if (bit > MAX_BIT)
    return;
  vec = bi->bi_vecs[bit];
  if (!vec)
    {
      vec = dk_alloc_box_zero (ALIGN_8 (100000 + (id / 8)), DV_STRING);
    }
  len = box_length (vec) - 1;
  if (id / 8 > len)
    {
      char *vec2 = dk_alloc_box_zero (ALIGN_8 (100000 + (id / 8)), DV_STRING);
      memcpy (vec2, vec, box_length (vec));
      vec = bi->bi_vecs[bit] = vec2;
    }
  vec[bit / 8] |= 1 << (id & 7);
}

void
bits_add (bits_inst_t * bi, int64 id, char *str)
{
  int n = -1;
  int64 int_id = ++bi->bi_last_int;
  int i;
  sethash ((void *) id, bi->bi_id_to_int, (void *) int_id);
  sethash ((void *) int_id, bi->bi_int_to_id, (void *) id);
  for (i = 0; str[i]; i++)
    {
      char c = str[i];
      if (!is_digit (c))
	{
	  if (-1 != n)
	    {
	      bits_set_bit (bi, int_id, n);
	      n = -1;
	    }
	}
      else
	{
	  if (-1 == n)
	    n = c - '0';
	  else
	    n *= c - '0';
	}
    }
  if (-1 != n)
    {
      bits_set_bit (bi, int_id, n);
    }
}



int
bits_open (char *cfg, slice_id_t slid, iext_index_t * ret, caddr_t * err_ret)
{
  char temp[MAX_BIT * 10];
  dk_session_t *dks;
  int fd;
  bits_inst_t *bi;
  char fname[100];
  if (bi)
    return 0;
  snprintf (fname, sizeof (fname), "%s.%d.bits", cfg, slid);
  bi = (bits_inst_t *) dk_alloc (sizeof (bits_inst_t));
  memset (bi, 0, sizeof (bits_inst_t));
  bi->bi_id_to_int = hash_table_allocate (1111);
  bi->bi_rw = rwlock_allocate ();
  bi->bi_int_to_id = hash_table_allocate (1111);
  bi->bi_file = box_dv_short_string (fname);
  bi->bi_vecs = (char **) dk_alloc_box (sizeof (caddr_t) * MAX_BIT, DV_ARRAY_OF_POINTER);
  *ret = bi;
  fd = open (bi->bi_file, O_RDWR | O_CREAT);
  dks = dk_session_allocate (SESCLASS_TCPIP);
  tcpses_set_fd (dks->dks_session, fd);
  CATCH_READ_FAIL (dks)
  {
    for (;;)
      {
	int64 id;
	bits_read_line (dks, temp, sizeof (temp));
	temp[sizeof (temp) - 1] = 0;
	id = atoi (temp);
	bits_read_line (dks, temp, sizeof (temp));
	temp[sizeof (temp) - 1] = 0;
	bits_add (bi, id, temp);
      }
  }
  END_READ_FAIL (dks);
  close (fd);
  return 0;
}



int
bits_checkpoint (bits_inst_t * bi, caddr_t * err_ret)
{
  int fill;
  char temp[MAX_BIT * 10];
  int fd;
  *err_ret = NULL;
  rwlock_rdlock (bi->bi_rw);
  fd = open (bi->bi_file, O_RDWR | O_CREAT);
  ftruncate (fd, 0);
  DO_HT (ptrlong, id, ptrlong, int_id, bi->bi_id_to_int)
  {
    int b;
    char *ptr = temp;
    sprintf (ptr, "%ld\n", id);
    fill = strlen (ptr);
    for (b = 0; b < MAX_BIT; b++)
      {
	int len;
	char *vec = bi->bi_vecs[b];
	if (!vec)
	  continue;
	len = box_length (vec) - 1;
	if (int_id / 8 >= len)
	  continue;
	if (vec[int_id / 8] & (1 << (int_id & 7)))
	  {
	    sprintf (ptr + fill, "%d ", b);
	    fill += strlen (ptr + fill);
	  }
      }
    sprintf (ptr + fill, "\n");
    fill += strlen (ptr + fill);
    write (fd, ptr, fill);
  }
  END_DO_HT;
  close (fd);
  rwlock_unlock (bi->bi_rw);
}


int
bits_insert (bits_inst_t * bi, iext_txn_t txn, int n_ins, int64 * ids, caddr_t * data, caddr_t * err_ret)
{
  int inx;
  rwlock_wrlock (bi->bi_rw);
  for (inx = 0; inx < n_ins; inx++)
    {
      bits_add (bi, ids[inx], data[inx]);
    }
  rwlock_unlock (bi->bi_rw);
  *err_ret = NULL;
  return 0;
}


void
bits_cr_free (bits_cr_t * cr)
{
  dk_free_box ((caddr_t) cr->bcr_bits);
  dk_free ((caddr_t) cr, sizeof (bits_cr_t));
}

int
bits_exec (bits_inst_t * bi, iext_txn_t txn, bits_cr_t ** cr_ret, struct client_connection_s *cli, char *str, void *params,
    caddr_t * err_ret)
{
  NEW_VARZ (bits_cr_t, cr);
  cr->bcr_bi = bi;
  cr->bcr_pos = BCR_INIT;
  if ('O' == str[0] || 'o' == str[0])
    {
      cr->bcr_mode = BCR_OR;
      if (is_digit (str[1]))
	cr->bcr_min_bits = atoi (str + 1);
      cr->bcr_mode = BCR_OR;
      str = strchr (str, ' ');
      if (!str)
	goto err;
    }
  else if ('A' == str[0] || 'a' == str[0])
    {
      cr->bcr_mode = BCR_AND;
      str++;
    }
  else
    {
      cr->bcr_mode = BCR_AND;
    }
  cr->bcr_bits = bits_parse (str);
  cr->bcr_n_bits = box_length (cr->bcr_bits) / sizeof (int);
  if (!cr->bcr_n_bits)
    goto err;
  *cr_ret = cr;
  return 0;
err:
  *err_ret = srv_make_new_error ("37000", "BITSN", "bad bits search string");
  return -1;
}

int
bits_next (bits_cr_t * cr, iext_txn_t * txn, int max, int *n_ret, int64 * ret, int64 * ret2, caddr_t * err_ret)
{
  int64 *vec;
  int64 word;
  int64 nth_word;
  int bit, nth_bit, nth2;
  int ret_ctr = 0;
  bits_inst_t *bi = cr->bcr_bi;
  if (BCR_AT_END == cr->bcr_pos)
    {
      *n_ret = 0;
      return 0;
    }
  rwlock_rdlock (bi->bi_rw);
  cr->bcr_pos++;
  for (;;)
    {
      nth_word = cr->bcr_pos / 64;
      nth_bit = cr->bcr_pos & 63;
      vec = (int64 *) bi->bi_vecs[cr->bcr_bits[0]];
    check_word:
      if (!vec || box_length (vec) / sizeof (int64) <= nth_word)
	goto at_end;
      word = vec[nth_word];
      if (!word)
	{
	  cr->bcr_pos = (nth_word + 1) * 64;
	  nth_word++;
	  goto check_word;
	}
      for (nth2 = 1; nth2 < cr->bcr_n_bits; nth2++)
	{
	  int64 *vec2 = (int64 *) bi->bi_vecs[cr->bcr_bits[nth2]];
	  if (!vec2 || box_length (vec2) <= nth_word)
	    goto at_end;
	  word &= vec2[nth_word];
	  if (!word)
	    {
	      nth_word++;
	      cr->bcr_pos = nth_word * 64;
	      goto check_word;
	    }
	}
      /* a non-zero and of the words in the position */
      for (bit = nth_bit; bit < 64; bit++)
	{
	  if (word & (1L << bit))
	    {
	      cr->bcr_pos = nth_word * 64 + bit;
	      ret[ret_ctr] = (int64) gethash ((void *) cr->bcr_pos, bi->bi_int_to_id);
	      ret2[ret_ctr++] = 1;
	      if (max == ret_ctr)
		{
		  rwlock_unlock (bi->bi_rw);
		  *n_ret = ret_ctr;
		  return 0;
		}
	    }
	}
      nth_word++;
      cr->bcr_pos = nth_word * 64;
    }
at_end:
  rwlock_unlock (bi->bi_rw);
  *n_ret = ret_ctr;
  cr->bcr_pos = BCR_AT_END;
  return 0;
}




void
bits_register ()
{
  index_type_t iext;
  memset (&iext, 0, sizeof (iext));
  iext.iext_name = "bits";
  iext.iext_open = bits_open;
  iext.iext_checkpoint = bits_checkpoint;
  iext.iext_insert = bits_insert;
  iext.iext_exec = bits_exec;
  iext.iext_cursor_free = bits_cr_free;
  iext.iext_next = bits_next;
  iext_register (&iext);
}
