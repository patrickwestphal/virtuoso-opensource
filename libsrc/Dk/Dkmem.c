/*
 *  Dkmem.c
 *
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


#include "Dk.h"
#include "Dk/Dksystem.h"
#include "Dksimd.h"



void
memzero (void* ptr, int len)
{
  memset (ptr, 0, len);
}


void
memset_16 (void* ptr, unsigned char fill, int len)
{
  memset (ptr, fill, len);
}


void
int_fill (int * ptr, int n, int len)
{
  unsigned int inx = 0;
  for (inx = inx; inx < len; inx++)
    ptr[inx] = n;
}


void
int64_fill (int64 * ptr, int64 n, int len)
{
  unsigned int inx = 0;
  for (inx = inx; inx < len; inx++)
    ptr[inx] = n;
}


void
int64_fill_nt (int64 * ptr, int64 n, int len)
{
  unsigned int inx = 0;
  for (inx = inx; inx < len; inx++)
    ptr[inx] = n;
}

void
int_asc_fill (int * ptr, int len, int start)
{
  unsigned int inx;
  for (inx = 0; inx < len; inx++)
    ptr[inx] = inx + start;
}


#define cpy(dtp) \
  {*(dtp*)target = *(dtp*)source; target += sizeof (dtp); source += sizeof (dtp);}

#define D_cpy(dtp) \
  { target -= sizeof (dtp); source -= sizeof (dtp); *(dtp*)target = *(dtp*)source;}

void
memcpy_16 (void * t, const void * s, size_t len)
{
  memcpy (t, s, len);
}


void
memcpy_16_nt (void * t, const void * s, size_t len)
{
  memcpy (t, s, len);
}

void
memmove_16 (void * t, const void * s, size_t len)
{
  memmove (t, s, len);
}



uint64
rdtsc()
{
#if defined(HAVE_GETHRTIME) || defined(SOLARIS)
  return (uint64) gethrtime ();
#elif defined (WIN32)
  return __rdtsc ();
#elif defined (__GNUC__) && defined (__x86_64__)
  uint32 lo, hi;

  /* Serialize */
  __asm__ __volatile__ ("xorl %%eax,%%eax\n\tcpuid":::"%rax", "%rbx", "%rcx", "%rdx");

  /* We cannot use "=A", since this would use %rax on x86_64 and return only the lower 32bits of the TSC */
  __asm__ __volatile__ ("rdtsc":"=a" (lo), "=d" (hi));

  return (uint64) hi << 32 | lo;
#elif defined (__GNUC__) && defined (__i386__)
  uint64 result;
  __asm__ __volatile__ ("rdtsc":"=A" (result));
  return result;
#elif defined(__GNUC__) && defined(__ia64__)
  uint64 result;
  __asm__ __volatile__ ("mov %0=ar.itc":"=r" (result));
  return result;
#elif defined(__GNUC__) && (defined(__powerpc__) || defined(__POWERPC__)) && (defined(__64BIT__) || defined(_ARCH_PPC64))
  uint64 result;
  __asm__ __volatile__ ("mftb %0":"=r" (result));
  return result;
#elif defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_REALTIME)
  {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (uint64) t.tv_sec * 1000000000 + (uint64) t.tv_nsec;
  }
  return 0;
#endif
}


char *
strstr_len (char * val, int len, char * pattern, int pattern_len)
{
  uint64 mask, head;
  int inx;
  if (pattern_len > len)
    return 0;
  len -= pattern_len;
  if (pattern_len < 8)
    {
      mask = ((uint64)-1) >> (8 * (8 - pattern_len));
      head = mask & *(int64*)pattern;
    }
  else
    {
      mask = (uint64)-1;
      head = *(int64*)pattern;
    }
  for (inx = 0; inx <= len; inx++)
    {
      if ((mask & *(int64*)(val + inx)) == head)
	{
	  if (pattern_len <= 8)
	    return val + inx;
	  memcmp_8 ((val + inx + 8), (pattern + 8), (pattern_len - 8), neq1);
	  return val + inx;
	neq1: ;
	}
    }

 neq:
  return NULL;
}


char *
strstr_sse42 (char * str, int str_len, char * substr, int substr_len)
{
#ifdef SSE42
  int last = str_len - substr_len;
  v16qi_t subs = (v16qi_t)__builtin_ia32_loadups ((float*)substr);
  int len1 = MIN (substr_len, 16), start;
  if (last < 0)
    return NULL;
  for (start = 0; start <= last; start += 16)
    {
      int len2, rc;
    again:
      len2 = str_len - start;
      rc = __builtin_ia32_pcmpestri128 (subs, len1, (v16qi_t)__builtin_ia32_loadups ((float*)(str + start)), len2, PSTR_EQUAL_ORDERED);
      if (rc < 16)
	{
	  char * s1 = str + start + 16, * s2;
	  int s2_len = substr_len - (16 - rc);
	  if (s2_len <= 0)
	    return str + start + rc;
	  s2 = substr + (16 - rc);
	  memcmp_8 (s1, s2, s2_len, neq);
	  return str + start + rc;
	neq:
	  start += rc + 1;
	  goto again;
	}
    }
  return NULL;
#else
  return strstr_len (str, str_len, substr, substr_len);
#endif
}

