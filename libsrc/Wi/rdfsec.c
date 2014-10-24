/*
 *  rdfsec.c
 *
 *  $Id$
 *
 *  RDF access scoping by in with chash
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

#include "sqlnode.h"
#include "arith.h"
#include "mhash.h"
#include "sqlparext.h"
#include "date.h"
#include "aqueue.h"
#include "sqlbif.h"
#include "datesupp.h"
#include "sqlpar.h"
#include "rdf_core.h"


id_hash_t *rdf_ctx_qrs;
dk_mutex_t rdf_ctx_qr_mtx;
dk_hash_t rdf_ctx_ht_ids;
extern user_t *user_t_dba;

int64 rdf_ctx_max_mem = 100000000;
int64 rdf_ctx_in_use;
long tc_rdf_ctx_hits;
long tc_rdf_ctx_misses;



void
rdf_sec_init ()
{
}

void
rdf_ctx_status ()
{
}

void
cli_ensure_sec (query_instance_t * qi, client_connection_t * cli)
{
}
