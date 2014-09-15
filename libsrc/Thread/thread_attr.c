/*
 *  thread_attr.c
 *
 *  $Id$
 *
 *  Manages thread local storage attributes
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
 *  
*/

#include "Dk.h"


void
_thread_init_attributes (thread_t *self)
{
  if (!self->thr_attributes)
    self->thr_attributes = hash_table_allocate (THREAD_ATTRIBUTE_HASH);
  else
    clrhash ((dk_hash_t *) self->thr_attributes);
}


void
_thread_free_attributes (thread_t *self)
{
  if (self->thr_attributes)
    {
      hash_table_free ((dk_hash_t *) self->thr_attributes);
      self->thr_attributes = NULL;
    }
}


void *
thread_setattr (thread_t *self, void *key, void *value)
{
  return sethash (key, (dk_hash_t *) self->thr_attributes, value);
}


void *
thread_getattr (thread_t *self, void *key)
{
  return gethash (key, (dk_hash_t *) self->thr_attributes);
}

/*#define ERR_DEBUG*/

#ifdef ERR_DEBUG
#define ARRAYP(a) \
  (IS_BOX_POINTER(a) && DV_ARRAY_OF_POINTER == box_tag((caddr_t) a))
#endif
caddr_t
thr_get_error_code (thread_t *thr)
{
  caddr_t ret = thr->thr_reset_code;
  thr->thr_reset_code = NULL;
#ifdef ERR_DEBUG
  fprintf (stderr, "thr_get_error_code (%p)=%p [%s]\n",
      thr, ret, ret ? (ARRAYP (ret) ? ((caddr_t *)ret)[2] : "<not found>") : "<none>");
#endif
  return ret;
}

void
thr_set_error_code (thread_t *thr, caddr_t err)
{
#ifdef ERR_DEBUG
  fprintf (stderr, "thr_set_error_code (%p, %p [%s]) -> free %p [%s]\n",
      thr, err, err ? (ARRAYP (err) ? ((caddr_t *)err)[2] : "<not found>") : "<none>",
      thr->thr_reset_code, thr->thr_reset_code ? (ARRAYP (thr->thr_reset_code) ?  ((caddr_t *)thr->thr_reset_code)[2] : "<not found>") : "<none>");
#endif
  dk_free_tree (thr->thr_reset_code);
  thr->thr_reset_code = err;
}


int
thread_parse_cpu_set (dk_cpu_set_t * cpu_set, char * str2)
{
#ifdef linux
  caddr_t  str = box_dv_short_string (str2);
  char * s;
  char * id;
  CPU_ZERO (cpu_set);
  id = strtok_r (str, " ", &s);
  while (id)
    {
      char * dash;
      if ((dash = strchr (id, '-')))
	{
	  int n, l, h = atoi (dash + 1);
	  *dash = 0;
	  l = atoi (id);
	  if (h >= CPU_SETSIZE)
	    return -1;
	  for (n = l; n <= h; n++)
	    CPU_SET (n, cpu_set);
	}
      else 
	{
	  int n = atoi (id);
	  if (n >= CPU_SETSIZE)
	    return -1;
	  CPU_SET (n, cpu_set);
	}
      id = strtok_r (NULL, " ", &s);
    }
  dk_free_box (str);
#endif
  return 0;
}


#ifdef linux
#include <sys/syscall.h>
#endif

int
thread_set_affinity (dk_cpu_set_t * cpu_set)
{
#ifdef linux
  int tid = syscall (SYS_gettid);
  return sched_setaffinity (tid, sizeof (cpu_set_t), cpu_set);
#else
  return 0;
#endif
}

dk_cpu_set_t thr_default_affinity;
int thr_is_default_affinity;

void 
thread_set_default_affinity (dk_cpu_set_t * cpu_set)
{
  if (!cpu_set)
    thr_is_default_affinity = 0;
  else
    {
      thr_default_affinity = *cpu_set;
      thr_is_default_affinity = 1;
    }
}


void
longjmp_brk (jmp_buf b, int rc)
{
  longjmp (b, rc);
}
