--
--  $Id$
--
--  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
--  project.
--
--  Copyright (C) 1998-2014 OpenLink Software
--
--  This project is free software; you can redistribute it and/or modify it
--  under the terms of the GNU General Public License as published by the
--  Free Software Foundation; only version 2 of the License, dated June 1991.
--
--  This program is distributed in the hope that it will be useful, but
--  WITHOUT ANY WARRANTY; without even the implied warranty of
--  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
--  General Public License for more details.
--
--  You should have received a copy of the GNU General Public License along
--  with this program; if not, write to the Free Software Foundation, Inc.,
--  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
--
--

create table DB.DBA.RDF_QUAD (
  G IRI_ID_8,
  S IRI_ID_8,
  P IRI_ID_8,
  O any,
  primary key (P, S, O, G) column
  )
alter index RDF_QUAD on DB.DBA.RDF_QUAD partition (S int (0hexffff00))

create distinct no primary key ref column index RDF_QUAD_SP on DB.DBA.RDF_QUAD (S, P) partition (S int (0hexffff00))
create column index RDF_QUAD_POGS on DB.DBA.RDF_QUAD (P, O, S, G) partition (O varchar (-1, 0hexffff))
create distinct no primary key ref column index RDF_QUAD_GS on DB.DBA.RDF_QUAD (G, S) partition (S int (0hexffff00))
create distinct no primary key ref column index RDF_QUAD_OP on DB.DBA.RDF_QUAD (O, P) partition (O varchar (-1, 0hexffff))
;

create table DB.DBA.RDF_QUAD_RECOV_TMP (
  G1 IRI_ID_8,  S1 IRI_ID_8,  P1 IRI_ID_8,  O1 any,  primary key (P1, S1, O1, G1) column)
alter index RDF_QUAD_RECOV_TMP on DB.DBA.RDF_QUAD_RECOV_TMP partition (S1 int (0hexffff00))
create column index RDF_QUAD_RECOV_TMP_POGS on DB.DBA.RDF_QUAD_RECOV_TMP (P1, O1, G1, S1) partition (O1 varchar (-1, 0hexffff))
create distinct no primary key ref column index RDF_QUAD_RECOV_TMP_OP on DB.DBA.RDF_QUAD_RECOV_TMP (O1, P1) partition (O1 varchar (-1, 0hexffff))
;

