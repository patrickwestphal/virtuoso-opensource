/*
 *  $Id$
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2014 OpenLink Software
 *
 *  This project is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; only version 2 of the License, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include "odbcinc.h"
#include "timeacct.h"

SQLHENV  henv = 0;
SQLHDBC  hdbc = 0;
SQLHSTMT hstmt = 0;

#define ARRAY_SIZE 1000
#define LINE_LEN 1024

int
ODBC_Connect (char *dsn, char *usr, char *pwd)
{
  SQLRETURN  retcode;

  retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

  if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
    {
      /* Set the ODBC version environment attribute */
      retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

      if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
	{
	  /* Allocate connection handle */
	  retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

	  if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
	    {

	      /* Connect to data source */
	      retcode = SQLConnect(hdbc, dsn, SQL_NTS, usr, SQL_NTS, pwd, SQL_NTS);

	      if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
		{
		  /* Allocate statement handle */
		  retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

		  if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
		    return 0;
		  SQLDisconnect(hdbc);
		}
	      SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
	    }
	}
      SQLFreeHandle(SQL_HANDLE_ENV, henv);
    }
  return -1;
}


int
ODBC_Disconnect (void)
{
  if (hstmt)
    SQLFreeStmt (hstmt, SQL_DROP);

  if (hdbc)
    SQLDisconnect (hdbc);

  if (hdbc)
    SQLFreeHandle (SQL_HANDLE_DBC, hdbc);

  if (henv)
    SQLFreeHandle (SQL_HANDLE_ENV, henv);

  return 0;
}


int
ODBC_Errors (char *where)
{
  unsigned char buf[250];
  unsigned char sqlstate[15];

  /*
   *  Get statement errors
   */
  while (SQLError (henv, hdbc, hstmt, sqlstate, NULL,
	buf, sizeof(buf), NULL) == SQL_SUCCESS)
    {
      fprintf (stdout, "%s ||%s, SQLSTATE=%s\n", where, buf, sqlstate);
    }

  /*
   *  Get connection errors
   */
  while (SQLError (henv, hdbc, SQL_NULL_HSTMT, sqlstate, NULL,
	buf, sizeof(buf), NULL) == SQL_SUCCESS)
    {
      fprintf (stdout, "%s ||%s, SQLSTATE=%s\n", where, buf, sqlstate);
    }

  /*
   *  Get environmental errors
   */
  while (SQLError (henv, SQL_NULL_HDBC, SQL_NULL_HSTMT, sqlstate, NULL,
	buf, sizeof(buf), NULL) == SQL_SUCCESS)
    {
      fprintf (stdout, "%s ||%s, SQLSTATE=%s\n", where, buf, sqlstate);
    }

  return -1;
}


int
Copy_data(char *fname)
{
  SQLCHAR *      Statement = "call copy_file (?, ?)";
  SQLCHAR        DataArray[ARRAY_SIZE][LINE_LEN];
  SQLULEN        IDIndArray[ARRAY_SIZE];
  SQLCHAR        DataArray2[ARRAY_SIZE][40];
  SQLULEN	 ParamsProcessed;
  char 		 str[LINE_LEN];
  FILE 	 	 *fp;
  int 	 	 inx, g_inx;

  fp = fopen (fname,"r");
  g_inx = 0;

  if (!fp)
    {
      fprintf (stdout, "Can't open file (%s)\n", fname);
      return -1;
    }

  while (!feof (fp))
    {
      for (inx=0; inx<ARRAY_SIZE; inx++)
	{
	  str[0]=0;
	  fgets (str, sizeof (str), fp);
	  if (!str[0])
	    break;

	  IDIndArray[inx] = SQL_NTS;
	  strcpy(DataArray[inx], str);
	  if (g_inx)
	    strcpy(DataArray2[inx], "");
	  else
	    strcpy(DataArray2[inx], fname);
	  g_inx ++;
	}

      if (SQLParamOptions(hstmt, inx, &ParamsProcessed) != SQL_SUCCESS)
	{
	  ODBC_Errors ("ODBC_Execute");
	  return -1;
	}

      if (SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, LINE_LEN, 0, DataArray, 0, IDIndArray)
	  != SQL_SUCCESS)
	{
	  ODBC_Errors ("ODBC_Execute");
	  return -1;
	}

      if (SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 40, 0, DataArray2, 0, IDIndArray)
	  != SQL_SUCCESS)
	{
	  ODBC_Errors ("ODBC_Execute");
	  return -1;
	}

      if (inx)
	{
	  if (SQLExecDirect(hstmt, Statement, SQL_NTS) != SQL_SUCCESS_WITH_INFO)
	    {
	      ODBC_Errors ("ODBC_Execute");
	      return -1;
	    }
	}
    }

  return 0;
}
;

int
Fill_data(char *fname)
{

  SQLCHAR *      Statement;
  SQLCHAR        DataArray[ARRAY_SIZE][LINE_LEN];
  SQLULEN        IDIndArray[ARRAY_SIZE];
  SQLULEN	 ParamsProcessed;
  char 		 str[LINE_LEN];
  char 		 p_name[20];
  char 		 call_code[200];
  FILE 	 	 *fp;
  int 	 	 inx, s_l;
  long 		 t_begin, t_end, t_lines, t_per, p_lines, t_total;

  fp = fopen (fname,"r");

  if (!fp)
    {
      fprintf (stdout, "Can't open file (%s)", fname);
      return -1;
    }

  fprintf (stdout, "Process: %s \n", fname);

  memset (p_name, 0, sizeof (p_name));
  s_l = strlen (strstr (fname, "."));
  strncpy (p_name, fname, strlen (fname) - s_l);

  snprintf (call_code, sizeof (call_code),
      "call i_%s (split_and_decode (?, 0, '\\0\\0|'))", p_name);

  Statement = call_code;
  t_lines = -1;
  p_lines = 0;

  while (!feof (fp))
    {
      t_lines ++;
      fgets (str, sizeof (str), fp);
    }

  rewind (fp);
/*SQLExecDirect (hstmt, (UCHAR *) "log_enable (0)", SQL_NTS);*/
  t_total = get_msec_count ();

  while (!feof (fp))
    {

      for (inx=0; inx<ARRAY_SIZE; inx++)
	{
	  str[0]=0;
	  fgets (str, sizeof (str), fp);
	  if (!str[0])
	    break;

	  IDIndArray[inx] = SQL_NTS;
	  strcpy(DataArray[inx], str);
	}


      if (SQLParamOptions(hstmt, inx, &ParamsProcessed) != SQL_SUCCESS)
	{
	  ODBC_Errors ("ODBC_Execute");
	  return -1;
	}


      if (SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, LINE_LEN, 0, DataArray, 0, IDIndArray)
	  != SQL_SUCCESS)
	{
	  ODBC_Errors ("ODBC_Execute");
	  return -1;
	}

      t_begin = get_msec_count ();
      if (inx)
	{
	  if (SQLExecDirect(hstmt, Statement, SQL_NTS) != SQL_SUCCESS_WITH_INFO)
	    {
	      ODBC_Errors ("ODBC_Execute");
	      return -1;
	    }
	  p_lines = p_lines + inx;
	  t_per = p_lines * 100 / t_lines;
	  fprintf (stdout, "%s\t%ld\trows\t%ld%%", fname, p_lines, t_per);
	  SQLTransact (hstmt, hdbc, SQL_COMMIT);
	  t_end = get_msec_count () - t_begin;
	  fprintf (stdout, "\t%ld msec.\n", t_end);
	}
    }

/*SQLExecDirect (hstmt, (UCHAR *) "log_enable (1)", SQL_NTS);*/

  fprintf (stdout, "Finish - %ld msec. \n", get_msec_count () - t_total);
  return 0;
}


int
main (int argc, char **argv)
{
  if (ODBC_Connect (argv[1], argv[2], argv[3]) != 0)
    {
      ODBC_Errors ("Connect Failed.");
    }

  if (!strncmp (argv[4], "-C", 2))
     Copy_data (argv[5]);
  else if (Fill_data (argv[4]) != 0)
     ODBC_Errors ("ODBC_Test");

  ODBC_Disconnect ();

  return 0;
}
