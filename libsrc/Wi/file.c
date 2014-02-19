
/* File tables */


#include "sqlnode.h"
#include "datesupp.h"
#include "sqlbif.h"
#include "aqueue.h"
#include "arith.h"


#define FSP_TEXT 0
#define FSP_AFTER_NL1 1
#define FSP_AFTER_ESC 2


#define dks_prev_recd(ses) (*(int64*)&(ses)->dks_fixed_thread_reqs)
#define dks_limit dks_bytes_sent
#define dks_copy_buf(ses) ((ses)->dks_out_buffer)

#define FT_MAX_FIELD 4000000


char *
dks_get_copy (dk_session_t * ses, int min_len, int *len_ret, int off)
{
  caddr_t copy = dks_copy_buf (ses);
  if (min_len > FT_MAX_FIELD)
    sqlr_new_error ("42000", "FTMAX", "CSV file table field length over %d, offset %ld", FT_MAX_FIELD, off + dks_prev_recd (ses));
  if (!copy)
    {
      int len = MAX (1000, 2 * min_len);
      copy = ses->dks_out_buffer = dk_alloc (len);
      ses->dks_out_length = len;
      *len_ret = ses->dks_out_length;
      return copy;
    }
  if (ses->dks_out_length > min_len * 2)
    {
      *len_ret = ses->dks_out_length;
      return ses->dks_out_buffer;
    }
  dk_free (ses->dks_out_buffer, ses->dks_out_length);
  ses->dks_out_buffer = dk_alloc (2 * min_len);
  *len_ret = ses->dks_out_length = 2 * min_len;
  return ses->dks_out_buffer;
}


char *
dks_extend_copy (dk_session_t * ses, int *len_ret, int off)
{
  caddr_t copy = ses->dks_out_buffer;
  caddr_t nc;
  if (ses->dks_out_length > FT_MAX_FIELD)
    sqlr_new_error ("42000", "FTMAX", "CSV file table field length over %d, offset %ld", FT_MAX_FIELD, off + dks_prev_recd (ses));
  nc = dk_alloc (2 * ses->dks_out_length);
  memcpy_16 (nc, copy, ses->dks_out_length);
  dk_free (copy, ses->dks_out_length);
  ses->dks_out_buffer = nc;
  *len_ret = ses->dks_out_length = 2 * ses->dks_out_length;
  return nc;
}


#define DKS_VARS(ses) \
int fill = ses->dks_in_fill - ses->dks_in_read; \
int inx = 0; \
  char * str = ses->dks_in_buffer + ses->dks_in_read; \
  char * copy = NULL; \
  int need_copy = 0, copy_len = 0, copy_fill = 0;\
  char c;

#define START_COPY  \
{ \
  copy = dks_get_copy (ses, inx, &copy_len, inx);	\
  memcpy_16 (copy, init_str, inx); \
  copy_fill = inx; \
}



#define NEXTC \
{ \
  if (inx == fill) \
    {\
      dks_prev_recd(ses) = ses->dks_bytes_received; \
      if (!copy && need_copy) \
	START_COPY; \
      fill = ses->dks_in_fill = service_read (ses, ses->dks_in_buffer, ses->dks_in_length, 0);\
      inx = 1;\
      ses->dks_in_read = 0;\
      str = ses->dks_in_buffer;\
      c = ses->dks_in_buffer[0];\
      if (fill == 0) \
	goto eof;\
    }\
  else \
    c = str[inx++];\
}

int
fs_field (file_source_t * fs, dk_session_t * ses, char **field_ret, int *len_ret)
{
  char eol = fs->fs_newline[0];
  char esc = fs->fs_escape;
  char delim = fs->fs_delim;
  DKS_VARS (ses) char *init_str = str;
  int init_fill = fill;
  need_copy = 1;
  for (;;)
    {
      NEXTC;
      if (delim == c || eol == c)
	{
	  if (copy)
	    {
	      *field_ret = copy;
	      *len_ret = copy_fill;
	    }
	  else
	    {
	      *field_ret = init_str;
	      *len_ret = inx - 1;
	    }
	  ses->dks_in_read += inx;
	  return 1;
	}
      if (esc == c)
	{
	  if (!copy)
	    START_COPY;
	  NEXTC;
	}
      if (copy)
	{
	  if (copy_fill == copy_len)
	    copy = dks_extend_copy (ses, &copy_len, inx + (str - ses->dks_in_buffer));
	  copy[copy_fill++] = c;
	}
    }
eof:
  if (copy)
    {
      *field_ret = copy;
      *len_ret = copy_fill;
    }
  else
    {
      *field_ret = init_str;
      *len_ret = inx;
    }
  return 0;
}

int
fs_skip_field (file_source_t * fs, dk_session_t * ses)
{
  char *fld;
  int len;
  return fs_field (fs, ses, &fld, &len);
}


int
fs_next_line (file_source_t * fs, dk_session_t * ses)
{
  char eol = fs->fs_newline[0];
  char esc = fs->fs_escape;
  char *init_str = NULL;
  DKS_VARS (ses);
  if (dks_prev_recd (ses) + ses->dks_in_read >= ses->dks_limit + 1)
    return 0;
  for (;;)
    {
      NEXTC;
      if (eol == c)
	{
	  ses->dks_in_read += inx;
	  return 1;
	}
      if (esc == c)
	{
	  NEXTC;
	}
    }
eof:
  return 0;
}



int64
fs_field_int (file_source_t * fs, char *field, int field_len, char *is_null, caddr_t * err_ret)
{
  int inx;
  int64 n = 0;
  int any_num = 0;
  if (!field_len)
    {
      *is_null = 1;
      return 0;
    }
  for (inx = 0; inx < field_len; inx++)
    {
      char c = field[inx];
      if (c >= '0' && c <= '9')
	{
	  any_num = 1;
	  n = n * 10 + (c - '0');
	}
    }
  if (!any_num)
    {
      *is_null = 1;
      return 0;
    }
  return n;
}



caddr_t
fs_field_numeric (file_source_t * fs, char *field, int field_len, char *is_null, caddr_t * err_ret)
{
  int rc;
  numeric_t n;
  if (!field_len)
    {
      *is_null = 1;
      return 0;
    }
  n = numeric_allocate ();
  field[field_len] = 0;
  rc = numeric_from_string (&n, field);
  if (NUMERIC_STS_SUCCESS != rc)
    {
      dk_free_box ((caddr_t) n);
      *is_null = 1;
      return NULL;
    }
  return n;
}


double
fs_field_double (file_source_t * fs, char *field, int field_len, char *is_null, caddr_t * err_ret)
{
  double d;
  if (!field_len)
    {
      *is_null = 1;
      return 0;
    }
  field[field_len] = 0;
  if (1 == sscanf (field, "%lf", &d))
    return d;
  *is_null = 1;
  return 0;
}


float
fs_field_float (file_source_t * fs, char *field, int field_len, char *is_null, caddr_t * err_ret)
{
  float d;
  if (!field_len)
    {
      *is_null = 1;
      return 0;
    }
  field[field_len] = 0;
  if (1 == sscanf (field, "%f", &d))
    return d;
  *is_null = 1;
  return 0;
}



void
dc_append_field (file_source_t * fs, data_col_t * dc, dbe_column_t * col, char *field, int field_len, caddr_t * err_ret)
{
  char is_null = 0;
  switch (col->col_sqt.sqt_dtp)
    {
    case DV_LONG_INT:
    case DV_INT64:
    case DV_SHORT_INT:
      ((int64 *) dc->dc_values)[dc->dc_n_values++] = fs_field_int (fs, field, field_len, &is_null, err_ret);
      break;
    case DV_SINGLE_FLOAT:
      ((float *) dc->dc_values)[dc->dc_n_values++] = fs_field_float (fs, field, field_len, &is_null, err_ret);
      break;
    case DV_DOUBLE_FLOAT:
      ((double *) dc->dc_values)[dc->dc_n_values++] = fs_field_double (fs, field, field_len, &is_null, err_ret);
      break;
    case DV_NUMERIC:
      ((caddr_t *) dc->dc_values)[dc->dc_n_values++] = fs_field_numeric (fs, field, field_len, &is_null, err_ret);
      break;
    case DV_STRING:
      {
	if (!field_len && fs->fs_null_empty_string)
	  {
	    is_null = 1;
	    break;
	  }
	dc_append_chars (dc, field, field_len);
	break;
      }
    case DV_DATETIME:
    case DV_DATE:
    case DV_TIMESTAMP:
    case DV_TIME:
      {
	caddr_t err_str = NULL;
	int dt_type;
	dtp_t dt[DT_LENGTH];
	if (!field_len)
	  {
	    caddr_t err_str = NULL;
	    is_null = 1;
	    break;
	  }
	switch (col->col_sqt.sqt_dtp)
	  {
	  case DV_DATE:
	    dt_type = DT_TYPE_DATE;
	    break;
	  case DV_DATETIME:
	  case DV_TIMESTAMP:
	    dt_type = DT_TYPE_DATETIME;
	    break;
	  case DV_TIME:
	    dt_type = DT_TYPE_TIME;
	    break;
	  default:
	    dt_type = -1;
	    break;
	  }
	field[field_len] = 0;

	iso8601_or_odbc_string_to_dt_1 (field, dt, 0x31ff, dt_type, &err_str);
	if (err_str)
	  {
	    is_null = 1;
	    break;
	  }
	memcpy_dt (dc->dc_values + DT_LENGTH * dc->dc_n_values, dt);
	dc->dc_n_values++;
	break;
      }
    default:
      is_null = 1;
      break;
    }
  if (is_null)
    dc_set_null (dc, dc->dc_n_values - 1);
  else
    {
      if (dc->dc_nulls)
	{
	  dc_ensure_null_bits (dc);
	  BIT_CLR (dc->dc_nulls, dc->dc_n_values - 1);
	}
    }
}


int
dc_col_cmp (data_col_t * dc, dbe_col_loc_t * cl, caddr_t value)
{
  collation_t *collation;
  int res;
  db_buf_t dv1, dv2;
  int inx;
  int l1;
  int l2;
  boxint n1, n2;
  int nth = dc->dc_n_values - 1;
  if (dc->dc_any_null)
    {
      if (DC_ANY_IS_NULL (dc, nth))
	return DVC_LESS;
    }
  switch (dc->dc_dtp)
    {
    case DV_LONG_INT:
    case DV_SHORT_INT:
    case DV_INT64:
      n1 = ((int64 *) dc->dc_values)[nth];
      switch (DV_TYPE_OF (value))
	{
	case DV_LONG_INT:
	  n2 = unbox_inline (value);
	  return NUM_COMPARE (n1, n2);
	case DV_SINGLE_FLOAT:
	  return cmp_double (((float) n1), *(float *) value, DBL_EPSILON);
	case DV_DOUBLE_FLOAT:
	  return cmp_double (((double) n1), *(double *) value, DBL_EPSILON);
	case DV_NUMERIC:
	  {
	    NUMERIC_VAR (n);
	    numeric_from_int64 ((numeric_t) & n, n1);
	    return (numeric_compare_dvc ((numeric_t) & n, (numeric_t) value));
	  }
	default:
	  {
	    log_error ("Unexpected param dtp=[%d]", DV_TYPE_OF (value));
	    GPF_T;
	  }
	}

    case DV_DATETIME:
    case DV_TIMESTAMP:
    case DV_DATE:
    case DV_TIME:
      dv1 = dc->dc_values + nth * DT_LENGTH;
      dv2 = (db_buf_t) value;
      for (inx = 0; inx < DT_COMPARE_LENGTH; inx++)
	{
	  if (dv1[inx] == dv2[inx])
	    continue;
	  if (dv1[inx] > dv2[inx])
	    return DVC_GREATER;
	  else
	    return DVC_LESS;
	}
      return DVC_MATCH;

    case DV_NUMERIC:
      {
	numeric_t n = ((numeric_t *) dc->dc_values)[nth];
	if (DV_DOUBLE_FLOAT == DV_TYPE_OF (value))
	  {
	    double d;
	    numeric_to_double ((numeric_t) & n, &d);
	    return cmp_double (d, *(double *) value, DBL_EPSILON);
	  }
	return (numeric_compare_dvc ((numeric_t) & n, (numeric_t) value));
      }
    case DV_SINGLE_FLOAT:
      {
	float flt = ((float *) dc->dc_values)[nth];
	switch (DV_TYPE_OF (value))
	  {
	  case DV_SINGLE_FLOAT:
	    return (cmp_double (flt, *(float *) value, FLT_EPSILON));
	  case DV_DOUBLE_FLOAT:
	    return (cmp_double (((double) flt), *(double *) value, DBL_EPSILON));
	  case DV_NUMERIC:
	    {
	      NUMERIC_VAR (n);
	      numeric_from_double ((numeric_t) & n, (double) flt);
	      return (numeric_compare_dvc ((numeric_t) & n, (numeric_t) value));
	    }
	  }
      }
    case DV_DOUBLE_FLOAT:
      {
	double dbl = ((double *) dc->dc_values)[nth];
	/* if the col is double, any arg is cast to double */
	return (cmp_double (dbl, *(double *) value, DBL_EPSILON));
      }
    case DV_IRI_ID_8:
    case DV_IRI_ID:
      {
	iri_id_t i1 = ((iri_id_t *) dc->dc_values)[nth];
	iri_id_t i2 = unbox_iri_id (value);
	res = NUM_COMPARE (i1, i2);
	return res;
      }
    default:
      if (DCT_BOXES & dc->dc_type)
	{
	  dv1 = ((db_buf_t *) dc->dc_values)[nth];
	  l1 = box_length (dv1);
	}
      else
	{
	  long hl, ll;
	  dv1 = ((db_buf_t *) dc->dc_values)[nth];
	  db_buf_length (dv1, &hl, &ll);
	  dv1 += hl;
	  l1 = ll;
	}
    }
  switch (cl->cl_sqt.sqt_dtp)
    {
    case DV_BIN:
      dv2 = (db_buf_t) value;
      l2 = box_length (dv2);
      return str_cmp_2 (dv1, dv2, NULL, l1, l2, 0, 0);
    case DV_STRING:
      collation = cl->cl_sqt.sqt_collation;
      dv2 = (db_buf_t) value;
      l2 = box_length_inline (dv2) - 1;
      inx = 0;
      if (collation)
	{
	  while (1)
	    {
	      if (inx == l1)
		{
		  if (inx == l2)
		    return DVC_MATCH;
		  else
		    return DVC_LESS;
		}
	      if (inx == l2)
		return DVC_GREATER;
	      if (collation->co_table[(unsigned char) dv1[inx]] < collation->co_table[(unsigned char) dv2[inx]])
		return DVC_LESS;
	      if (collation->co_table[(unsigned char) dv1[inx]] > collation->co_table[(unsigned char) dv2[inx]])
		return DVC_GREATER;
	      inx++;
	    }
	}
      else
	return str_cmp_2 (dv1, dv2, NULL, l1, l2, 0, 0);
    case DV_WIDE:
    case DV_LONG_WIDE:
      {
	/* the param is cast to narrow utf8 */
	dv2 = (db_buf_t) value;
	l2 = box_length (dv2) - 1;
	return str_cmp_2 (dv1, dv2, NULL, l1, l2, 0, 0);
      }
    case DV_ANY:
      dv2 = (db_buf_t) value;
      return (dv_compare (dv1, dv2, cl->cl_sqt.sqt_collation, 0));
    default:
      GPF_T1 ("type not supported in comparison");
    }
  return 0;
}


int
dc_like_compare (data_col_t * dc, caddr_t pattern, search_spec_t * spec)
{
  long hl, ll;
  char temp[MAX_ROW_BYTES];
  int res, st = LIKE_ARG_CHAR, pt = LIKE_ARG_CHAR;
  dtp_t dtp2 = DV_TYPE_OF (pattern), dtp1;
  int len1;
  db_buf_t dv1;
  collation_t *collation = spec->sp_collation;
  dbe_col_loc_t *cl = &spec->sp_cl;
  if (dtp_is_fixed (cl->cl_sqt.sqt_dtp))	/* if by chance numeric column then no match */
    return DVC_LESS;
  dv1 = ((db_buf_t *) dc->dc_values)[dc->dc_n_values - 1];
  dtp1 = dv1[0];
  db_buf_length (dv1, &hl, &ll);
  len1 = ll;
  dv1 += hl;
  if (dtp2 != DV_SHORT_STRING && dtp2 != DV_LONG_STRING && dtp2 != DV_WIDE && dtp2 != DV_LONG_WIDE)
    return DVC_LESS;
  switch (dtp2)
    {
    case DV_WIDE:
    case DV_LONG_WIDE:
      pt = LIKE_ARG_WCHAR;
      break;
    }
  switch (dtp1)
    {
    case DV_STRING:
    case DV_SHORT_STRING_SERIAL:
      if (collation && collation->co_is_wide)
	collation = NULL;
      break;
    case DV_WIDE:
      st = LIKE_ARG_UTF;
      collation = NULL;
      break;
    case DV_LONG_WIDE:
      st = LIKE_ARG_UTF;
      collation = NULL;
      break;
    default:
      return DVC_LESS;
    }
  if (dc->dc_buf_fill < dc->dc_buf_len)
    {
      dv1[len1] = 0;
      return cmp_like (dv1, pattern, collation, spec->sp_like_escape, st, pt);
    }
  if (len1 > sizeof (temp) - 1)
    {
      caddr_t temp_str = dk_alloc_box (len1 + 1, DV_STRING);
      memcpy_16 (temp_str, dv1, len1);
      temp_str[len1] = 0;
      res = cmp_like (temp_str, pattern, collation, spec->sp_like_escape, st, pt);
      dk_free_box (temp_str);
      return res;
    }
  memcpy_16 (temp, dv1, len1);
  temp[len1] = 0;
  return cmp_like (temp, pattern, collation, spec->sp_like_escape, st, pt);
}


int
fs_check (file_source_t * fs, caddr_t * inst, search_spec_t * sp, data_col_t * dc)
{
  caddr_t *params = (caddr_t *) QST_GET_V (inst, fs->fs_search_params);
  for (sp = sp; sp; sp = sp->sp_next)
    {
      dtp_t op = sp->sp_min_op;
      if (DVC_CMP_MASK & op)
	{
	  int res = dc_col_cmp (dc, &sp->sp_cl, params[sp->sp_min]);
	  if (0 == (op & res) || (DVC_NOORDER & res))
	    return DVC_LESS;
	}
      else if (op == CMP_LIKE)
	{
	  if (DVC_MATCH != dc_like_compare (dc, params[sp->sp_min], sp))
	    return DVC_LESS;
	  goto next_sp;
	}
      if (sp->sp_max_op != CMP_NONE)
	{
	  int res = dc_col_cmp (dc, &sp->sp_cl, params[sp->sp_max]);
	  if (0 == (sp->sp_max_op & res) || (DVC_NOORDER & res))
	    return DVC_LESS;
	}
    next_sp:;
    }
  return DVC_MATCH;
}


int
fs_next_row (file_source_t * fs, caddr_t * inst)
{
  dk_session_t *ses = (dk_session_t *) QST_GET (inst, fs->fs_ses);
  caddr_t err = NULL;
  char *field;
  int field_len, inx;
  int nth_field = 0;
  int n_out = box_length (fs->fs_out_pos) / sizeof (ptrlong);
  int nth;
  if (dks_prev_recd (ses) + ses->dks_in_read >= ses->dks_limit + 1)
    return DVC_INDEX_END;
  for (nth = 0; nth < n_out; nth++)
    {
      int pos = fs->fs_out_pos[nth];
      for (; nth_field < pos; nth_field++)
	{
	  if (!fs_skip_field (fs, ses))
	    goto no_row;
	}
      if (!fs_field (fs, ses, &field, &field_len))
	goto no_row;
      nth_field++;
      if (fs->fs_out_slots[nth])
	{
	  data_col_t *out_dc = QST_BOX (data_col_t *, inst, fs->fs_out_slots[nth]->ssl_index);
	  dc_append_field (fs, out_dc, fs->fs_out_cols[nth], field, field_len, &err);

	  if (fs->fs_specs[nth] && DVC_MATCH != fs_check (fs, inst, fs->fs_specs[nth], out_dc))
	    goto next_row;

	}
    }
  fs_next_line (fs, ses);
  return DVC_MATCH;
next_row:
  for (inx = 0; inx <= nth; inx++)
    {
      data_col_t *dc = QST_BOX (data_col_t *, inst, fs->fs_out_slots[inx]->ssl_index);
      dc_pop_last (dc);
    }
  if (fs_next_line (fs, ses))
    return DVC_LESS;
  return DVC_INDEX_END;
no_row:
  if (fs_next_line (fs, ses))
    return DVC_LESS;
  return DVC_INDEX_END;
}


int
fs_set_range (file_source_t * fs, caddr_t * inst, int n_slices, int nth_slice)
{
  dk_session_t *ses = (dk_session_t *) QST_GET_V (inst, fs->fs_ses);
  int fd = tcpses_get_fd (ses->dks_session);
  int64 len = lseek (fd, 0, SEEK_END);
  int64 low = unbox (qst_get (inst, fs->fs_start_offset));
  int64 high = unbox (qst_get (inst, fs->fs_limit));
  int64 size, slice, low2, rc;
  if (-1 == high)
    high = len;
  if (high > len)
    high = len;
  if (low >= high)
    return 0;
  size = high - low;
  slice = size / n_slices;
  low2 = low + (slice * nth_slice);
  rc;
  rc = lseek (fd, low2, SEEK_SET);
  ses->dks_bytes_received = 0;
  ses->dks_in_fill = 0;
  ses->dks_in_read = 0;
  ses->dks_limit = nth_slice == n_slices - 1 ? INT64_MAX - 10 : slice;
  CATCH_READ_FAIL (ses)
  {
    if (low2)
      rc = fs_next_line (fs, ses);
    else
      rc = 1;
  }
  FAILED
  {
    sqlr_new_error ("42000", "FTBIO", "Read error on file table");
  }
  END_READ_FAIL (ses);
  if (!rc)
    return 0;
  return 1;
}


int
ft_set_cl_file (file_source_t * fs, QI * qi, caddr_t * file_name)
{
  caddr_t *arr = (caddr_t *) * file_name;
  int len = BOX_ELEMENTS (arr);
  int inx;
  cl_host_group_t *chg = qi->qi_client->cli_csl->csl_chg;
  if ((len & 1))
    sqlr_new_error ("42000", "FS...", "odd length of file name array");
  for (inx = 0; inx + 1 < len; inx += 2)
    {
      caddr_t *nos = (caddr_t *) arr[inx + 1];
      dtp_t dtp = DV_TYPE_OF (nos);
      if (!DV_STRINGP (arr[inx]))
	sqlr_new_error ("420000", "FS....", "File nam,e in file name array is not a string");
      if (DV_LONG_INT == dtp)
	{
	  if (local_cll.cll_this_host != unbox (nos))
	    goto next;
	  qst_set_long ((caddr_t *) qi, fs->fs_n_slices, BOX_ELEMENTS (chg->chg_hosted_slices));
	  qst_set_long ((caddr_t *) qi, fs->fs_nth_slice, box_position ((caddr_t *) chg->chg_hosted_slices,
		  (caddr_t) qi->qi_client->cli_csl));
	  *file_name = arr[inx];
	  return 1;
	}
      else if (DV_ARRAY_OF_POINTER == dtp)
	{
	  caddr_t this_slice = (caddr_t) (uptrlong) qi->qi_client->cli_slice;
	  int pos = box_position (nos, this_slice);
	  if (-1 != pos)
	    {
	      qst_set_long ((caddr_t *) qi, fs->fs_nth_slice, pos);
	      qst_set_long ((caddr_t *) qi, fs->fs_n_slices, BOX_ELEMENTS (nos));
	      *file_name = arr[inx];
	      return 1;
	    }
	}
      else
	sqlr_new_error ("42000", "FS...",
	    "The element after the file name in a file list must be either a host number of an array of slice numbers");
    next:;
    }
  return 0;
}



int fs_file_in_buf_len = 2048 * 1024;

int
ft_ts_start_search (table_source_t * ts, caddr_t * inst)
{
  QNCAST (QI, qi, inst);
  key_source_t *ks = ts->ts_order_ks;
  int is_nulls, is_cl = 0;
  it_cursor_t itc_auto;
  it_cursor_t *itc = &itc_auto;
  dk_session_t *ses;
  file_source_t *fs = ts->ts_fs;
  dbe_table_t *tb = fs->fs_table;
  int set_save = qi->qi_set;
  file_table_t *ft = tb->tb_ft;
  caddr_t file_name;
  int fd;
  qi->qi_set = 0;
  file_name = fs->fs_file_name ? qst_get (inst, fs->fs_file_name) : ft->ft_file;
  ITC_INIT (itc, NULL, NULL);
  itc_from (itc, ks->ks_key, qi->qi_client->cli_slice);
  qi->qi_set = set_save;
  if (!ks->ks_is_qf_first)
    {
      ks_vec_params (ks, itc, inst);
    }
  else
    {
      if (ks->ks_cl_local_cast)
	ks_cl_local_cast (ks, inst);
    }
  is_nulls = ks_make_spec_list (itc, ks->ks_spec.ksp_spec_array, inst);
  is_nulls |= ks_make_spec_list (itc, ks->ks_row_spec, inst);
  if (is_nulls)
    return 0;
  if (QI_NO_SLICE != qi->qi_client->cli_slice && qi->qi_client->cli_csl)
    {
      /* cluster case.  If just file name, divide according to n slices and ordinal of this slice.  If array of file names, take the name that passes with this slice and divide among the slices with the file */
      is_cl = 1;
      if (DV_STRINGP (file_name))
	{
	  int ordinal = qi->qi_client->cli_csl ? qi->qi_client->cli_csl->csl_ordinal : 0;
	  int n_phys = qi->qi_client->cli_csl->csl_clm->clm_id_to_slice->ht_count;
	  qst_set_long (inst, fs->fs_n_slices, n_phys);
	  qst_set_long (inst, fs->fs_nth_slice, ordinal);
	}
      else if (DV_ARRAY_OF_POINTER == DV_TYPE_OF (file_name))
	{
	  if (!ft_set_cl_file (fs, qi, &file_name))
	    return 0;
	}
    }
  file_path_assert (file_name, NULL, 0);
  fd = open (file_name, OPEN_FLAGS_RO);
  if (-1 == fd)
    {
      if (fs->fs_error_if_no_file)
	sqlr_new_error ("42000", "FTOPE", "Cannot open file %s for file table read", file_name);
      return 0;
    }
  ses = dk_session_alloc_box (SESCLASS_TCPIP, fs_file_in_buf_len);
  tcpses_set_fd (ses->dks_session, fd);
  qst_set (inst, fs->fs_ses, (caddr_t) ses);
  if (fs->fs_nth_slice && (is_cl || unbox (qst_get (inst, fs->fs_nth_slice))))
    {
      return fs_set_range (fs, inst, unbox (qst_get (inst, fs->fs_n_slices)), unbox (qst_get (inst, fs->fs_nth_slice)));
    }
  ses->dks_limit = INT64_MAX - 10;
  return 1;
}


void
fs_set_params (file_source_t * fs, caddr_t * inst)
{
  caddr_t *box;
  int inx, fill = 0;
  if (!fs->fs_n_params)
    return;
  box = (caddr_t *) dk_alloc_box_zero (sizeof (caddr_t) * fs->fs_n_params, DV_BIN);
  DO_BOX (search_spec_t *, sp, inx, fs->fs_specs)
  {
    for (sp = sp; sp; sp = sp->sp_next)
      {
	if (CMP_NONE != sp->sp_min_op)
	  box[sp->sp_min] = qst_get (inst, sp->sp_min_ssl);
	if (CMP_NONE != sp->sp_max_op)
	  box[sp->sp_max] = qst_get (inst, sp->sp_max_ssl);
      }
  }
  END_DO_BOX;
  qst_set (inst, fs->fs_search_params, box);
}


extern long tc_qp_thread;

void
ft_ts_thread (table_source_t * ts, caddr_t * inst, int aq_state, int inx)
{
  /* add a thread to ts_aq running over itc */
  QNCAST (query_instance_t, qi, inst);
  file_source_t *fs = ts->ts_fs;
  caddr_t *cp_inst;
  query_instance_t *cp_qi;
  async_queue_t *aq = (async_queue_t *) QST_GET_V (inst, ts->ts_aq);
  caddr_t **qis = (caddr_t **) QST_GET_V (inst, ts->ts_aq_qis);
  TC (tc_qp_thread);
  qi->qi_client->cli_activity.da_qp_thread++;
  if (!aq)
    {
      aq = aq_allocate (qi->qi_client, enable_qp * 1.5);
      aq->aq_do_self_if_would_wait = 1;
      lt_timestamp (qi->qi_trx, &aq->aq_lt_timestamp[0]);
      aq->aq_ts = get_msec_real_time ();
      aq->aq_row_autocommit = qi->qi_client->cli_row_autocommit;
      aq->aq_non_txn_insert = qi->qi_non_txn_insert;
      qst_set (inst, ts->ts_aq, (caddr_t) aq);
    }
  if (!qis || BOX_ELEMENTS (qis) <= inx)
    {
      caddr_t **new_qis = (caddr_t **) dk_alloc_box_zero (sizeof (caddr_t) * (inx + 20), DV_ARRAY_OF_POINTER);
      if (qis)
	memcpy (new_qis, qis, box_length (qis));
      QST_BOX (caddr_t **, inst, ts->ts_aq_qis->ssl_index) = new_qis;
      dk_free_box ((caddr_t) qis);
      qis = new_qis;
    }
  cp_inst = qis[inx];
  if (!cp_inst)
    cp_inst = qst_copy (inst, ts->ts_branch_ssls, ts->ts_branch_sets);
  else
    GPF_T1 ("cannot reuse a qi for qp.  Make new instead");
  qis[inx] = cp_inst;
  cp_qi = (query_instance_t *) cp_inst;
  qst_set_long (cp_inst, fs->fs_n_slices, unbox (qst_get (inst, fs->fs_n_slices)));
  qst_set_long (cp_inst, fs->fs_nth_slice, inx);
  if (!ts->ts_agg_node)
    ;
  else if (IS_QN (ts->ts_agg_node, select_node_input_subq))
    ((query_instance_t *) cp_inst)->qi_branched_select = (select_node_t *) ts->ts_agg_node;
  else
    {
      /* making a copy for aggregation, can be that agg is already inited in original, so reset it */
      QNCAST (fun_ref_node_t, fref, ts->ts_agg_node);
      int n_sets = QST_INT (inst, fref->src_gen.src_prev->src_out_fill);
      qn_init ((table_source_t *) ts->ts_agg_node, cp_inst);
      cp_qi->qi_n_sets = n_sets;
      fun_ref_set_defaults_and_counts (fref, cp_inst);
      fun_ref_reset_setps (fref, cp_inst);
    }
  QST_INT (cp_inst, ts->ts_aq_state) = aq_state;
  SRC_IN_STATE (ts, cp_inst) = cp_inst;
  fs_set_params (fs, cp_inst);
  aq_request (aq, aq_qr_func, list (4, box_copy ((caddr_t) cp_inst), box_num ((ptrlong) ts->src_gen.src_query),
	  box_num (qi->qi_trx->lt_rc_w_id ? qi->qi_trx->lt_rc_w_id : qi->qi_trx->lt_w_id),
	  box_num ((ptrlong) qi->qi_client->cli_csl)));
}


int
ft_ts_split (table_source_t * ts, caddr_t * inst, int n_sets)
{
  QNCAST (QI, qi, inst);
  int fd, nth;
  file_source_t *fs = ts->ts_fs;
  dk_session_t *ses;
  int rc;
  int64 high, low, n_slices, len;
  qst_set_long (inst, fs->fs_n_slices, 0);
  qst_set_long (inst, fs->fs_nth_slice, 0);
  rc = ft_ts_start_search (ts, inst);
  if (!rc)
    return 0;
  if (QI_NO_SLICE != qi->qi_client->cli_slice && !ts->ts_aq)
    return 1;			/* cluster slice, fd already positioned */
  ses = (dk_session_t *) QST_GET_V (inst, fs->fs_ses);
  fd = tcpses_get_fd (ses->dks_session);
  len = lseek (fd, 0, SEEK_END);
  low = unbox (qst_get (inst, fs->fs_start_offset));
  high = unbox (qst_get (inst, fs->fs_limit));
  if (-1 == high)
    high = len;
  if (high > len)
    high = len;
  if (low >= high)
    return 0;
  if (!ts->ts_aq)
    n_slices = 1;
  else
    {
      if (!qi->qi_is_branch && !qi->qi_root_id)
	qi_assign_root_id (qi);
      n_slices = MIN (enable_qp, high - low);
      n_slices = 1 + qi_inc_branch_count (qi, enable_qp, n_slices - 1);
    }
  qst_set_long (inst, fs->fs_n_slices, n_slices);
  fs_set_range (fs, inst, n_slices, 0);
  for (nth = 1; nth < n_slices; nth++)
    {
      ft_ts_thread (ts, inst, TS_AQ_FIRST, nth);
    }
  if (n_slices > 1 && ts->ts_aq_state)
    QST_INT (inst, ts->ts_aq_state) = TS_AQ_COORD;
  return 1;
}


caddr_t
ft_ts_handle_aq (table_source_t * ts, caddr_t * inst, caddr_t * state, int *n_sets_ret)
{
  /* called when continuing a file ts with an aq.  Return inst if ts should start, null if should continue from position, -1 if should return */
  int aq_state;
  QNCAST (query_instance_t, qi, inst);
  if (!ts || *n_sets_ret > 1)
    return state;
  if (!ts->ts_aq)
    return NULL;
  aq_state = QST_INT (inst, ts->ts_aq_state);
  switch (aq_state)
    {
    case TS_AQ_SRV_RUN:
      return NULL;
    case TS_AQ_FIRST:
      {
	QST_INT (inst, ts->ts_aq_state) = TS_AQ_SRV_RUN;
	ft_ts_start_search (ts, inst);
	*n_sets_ret = 1;
	return NULL;
      }
    case TS_AQ_COORD:
      return NULL;
    case TS_AQ_COORD_AQ_WAIT:
      {
	/* coordinating ts has done its part.  Wait for the aq branches */
	async_queue_t *aq = QST_BOX (async_queue_t *, inst, ts->ts_aq->ssl_index);
	caddr_t err1 = NULL, err2 = NULL;
	IO_SECT (inst);
	aq->aq_wait_qi = qi;
	aq_wait_all (aq, &err1);
	END_IO_SECT (&err2);
	if (err1)
	  {
	    dk_free_tree (err2);
	    sqlr_resignal (err1);
	  }
	if (err2)
	  sqlr_resignal (err2);
	SRC_IN_STATE (ts, inst) = NULL;
	ts_aq_result (ts, inst);
	if (qi->qi_client->cli_activity.da_anytime_result)
	  cli_anytime_timeout (qi->qi_client);
	return 1;
      }
    }
  return NULL;
}



void
file_source_input (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  QNCAST (QI, qi, inst);
  file_source_t *fs = ts->ts_fs;
  dk_session_t *ses;
  int n_sets = ts->src_gen.src_prev ? QST_INT (inst, ts->src_gen.src_prev->src_out_fill) : qi->qi_n_sets;
  int nth_set, batch_sz;
  int rc = 0;
  if (state)
    {
      nth_set = QST_INT (inst, ts->clb.clb_nth_set) = 0;
      fs_set_params (fs, inst);
      if (!ft_ts_split (ts, inst, n_sets))
	return;
      state = NULL;
    }
  else
    nth_set = QST_INT (qi, ts->clb.clb_nth_set);

again:
  batch_sz = QST_INT (inst, ts->src_gen.src_batch_size);
  QST_INT (inst, ts->src_gen.src_out_fill) = 0;
  dc_reset_array (inst, (data_source_t *) ts, fs->fs_out_slots, -1);

  state = ft_ts_handle_aq (ts, inst, state, &n_sets);

  ses = (dk_session_t *) QST_GET_V (inst, fs->fs_ses);
  for (; nth_set < n_sets; nth_set++)
    {
      for (;;)
	{
	  qi->qi_set = nth_set;
	  QST_INT (inst, ts->clb.clb_nth_set) = nth_set;
	  if (state)
	    {
	      rc = ft_ts_start_search (ts, inst);
	      if (!rc)
		return;
	      ses = (dk_session_t *) QST_GET_V (inst, fs->fs_ses);
	      state = NULL;
	    }
	  CATCH_READ_FAIL (ses)
	  {
	    rc = fs_next_row (fs, inst);
	  }
	  FAILED
	  {
	    rc = DVC_INDEX_END;
	    if (!DKSESSTAT_ISSET (ses, SST_BROKEN_CONNECTION))
	      sqlr_new_error ("42000", "FSDIO", "File read error on file table %s", fs->fs_table->tb_name);
	  }
	  END_READ_FAIL (ses);
	  if (DVC_MATCH == rc)
	    qn_result ((data_source_t *) ts, inst, nth_set);
	  else if (DVC_LESS == rc)
	    continue;
	  else if (DVC_INDEX_END == rc)
	    {
	      SRC_IN_STATE (ts, qi) = NULL;
	      break;
	    }

	  SRC_IN_STATE (ts, inst) = inst;
	  state = NULL;
	  if (QST_INT (inst, ts->src_gen.src_out_fill) >= batch_sz)
	    {
	      QST_INT (inst, ts->clb.clb_nth_set) = nth_set;
	      qn_send_output ((data_source_t *) ts, (caddr_t *) qi);
	      goto again;
	    }
	}
    next_set:
      state = inst;
    }
  SRC_IN_STATE (ts, inst) = NULL;
  ts_aq_handle_end (ts, inst);
  if (QST_INT (inst, ts->src_gen.src_out_fill))
    {
      qn_send_output ((data_source_t *) ts, (caddr_t *) qi);
    }
  ts_aq_final (ts, inst, NULL);
}




caddr_t
dk_base_tlsf_string (char *str)
{
  caddr_t box;
  int len = strlen (str);
  WITH_TLSF (base_tlsf)
  {
    box = dk_alloc_box (len + 1, DV_STRING);
    memcpy (box, str, len);
    box[len] = 0;
  }
  END_WITH_TLSF;
  return box;
}


int
ft_string_opt (char *name, caddr_t * opts, caddr_t * ret, int must_have)
{
  int inx, len = BOX_ELEMENTS (opts);
  for (inx = 0; inx < len - 1; inx += 2)
    {
      caddr_t opt = opts[inx];
      if (!DV_STRINGP (opt))
	continue;
      if (!stricmp (opt, name))
	{
	  caddr_t str = opts[inx + 1];
	  if (!DV_STRINGP (str))
	    break;
	  *ret = dk_base_tlsf_string (str);
	  return 1;
	}
    }
  if (must_have)
    sqlr_new_error ("42000", "FTOPT", "File table required option %s not given", name);
  return 0;
}


int
ft_char_opt (char *name, caddr_t * opts, char *ret, int must_have)
{
  int inx, len = BOX_ELEMENTS (opts);
  for (inx = 0; inx < len - 1; inx += 2)
    {
      caddr_t opt = opts[inx];
      if (!DV_STRINGP (opt))
	continue;
      if (!stricmp (opt, name))
	{
	  caddr_t str = opts[inx + 1];
	  if (!DV_STRINGP (str))
	    break;
	  *ret = str[0];
	  return 1;
	}
    }
  if (must_have)
    sqlr_new_error ("42000", "FTOPT", "File table required option %s not given", name);
  return 0;
}


caddr_t
bif_ft_file_table (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  file_table_t tft;
  file_table_t *ft;
  char *tb_name = bif_string_arg (qst, args, 0, "ft_file_table");
  char *file = bif_string_or_null_arg (qst, args, 1, "ft_file_table");
  caddr_t *opts = bif_array_of_pointer_arg (qst, args, 2, "ft_file_table");
  int no_error = bif_long_arg (qst, args, 3, "ft_file_table");
  dbe_table_t *tb = sch_name_to_table (wi_inst.wi_schema, tb_name);
  if (!tb)
    {
      if (no_error)
	return NULL;
      sqlr_new_error ("42000", "FTDEF", "No table %s for file definition", tb_name);
    }
  if (!file)
    {
      /* no freeing, refs may exist in compiled stmts or execution */
      tb->tb_ft = NULL;
      return NULL;
    }
  key_ensure_visible_parts (tb->tb_primary_key);
  memzero (&tft, sizeof (tft));
  ft = &tft;
  ft->ft_file = dk_base_tlsf_string (file);
  ft_string_opt ("newline", opts, &ft->ft_newline, 1);
  ft_char_opt ("delimiter", opts, &ft->ft_delim, 1);
  ft_char_opt ("escape", opts, &ft->ft_escape, 0);
  ft = (file_table_t *) tlsf_base_alloc (sizeof (file_table_t));
  memcpy (ft, &tft, sizeof (file_table_t));
  tb->tb_ft = ft;
  return NULL;
}




void
file_init ()
{
  bif_define ("ft_file_table", bif_ft_file_table);

}
