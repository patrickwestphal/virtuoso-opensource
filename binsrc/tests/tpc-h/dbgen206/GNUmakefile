#
# $Id$
#
#

include ../../../../Makeconfig
include ../../../../Makerules
include ../../../../virtappl.inc
include ../../../../cplusplus.inc

# Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS, 
#                                  SGI, SUN, U2200, VMS, LINUX, WIN32

DATABASE=  VIRTUOSO

ifdef LINUX_OPSYS
MACHINE=LINUX
endif

ifdef Darwin_OPSYS
endif
MACHINE=LINUX

ifdef SOLARIS_OPSYS
MACHINE =  SUN
endif

WORKLOAD = TPCH 
CFLAGS+= -DDBNAME=\"dss\" -D$(MACHINE) -D$(DATABASE) -D$(WORKLOAD) -DUNIX
CFLAGS+= -I$(TOP)/libsrc/Wi -I$(TOP)/libsrc/plugin -I$(TOP)/binsrc/tests
LDFLAGS+= -L$(TOP)/lib
OBJ     = .o
EXE     =
LIBS    = -lm -lwic -ldk1t -lthrs -lutil2  -lssl -lcrypto
VERSION=2
RELEASE=6
PATCH=0
BUILD=`grep BUILD release.h | cut -f3 -d' '`
NEW_BUILD=`expr ${BUILD} + 1`
#
PROG1 = dbgen$(EXE)
PROG2 = qgen$(EXE)
PROG3 = load_tbl$(EXE_SUFFIX)
PROGS = $(PROG1) $(PROG2) $(PROG3)
#
HDR1 = dss.h rnd.h config.h dsstypes.h shared.h bcd2.h rng64.h release.h
HDR2 = tpcd.h permute.h
HDR  = $(HDR1) $(HDR2)
#
SRC1 = build.c driver.c bm_utils.c rnd.c print.c load_stub.c bcd2.c \
	speed_seed.c text.c permute.c rng64.c
SRC2 = qgen.c varsub.c 
SRC3 = load_tbl.c time.c 
SRC  = $(SRC1) $(SRC2) $(SRC3)
#
OBJ1 = build$(OBJ) driver$(OBJ) bm_utils$(OBJ) rnd$(OBJ) print$(OBJ) \
	load_stub$(OBJ) bcd2$(OBJ) speed_seed$(OBJ) text$(OBJ) permute$(OBJ) \
	rng64$(OBJ)
OBJ2 = build$(OBJ) bm_utils$(OBJ) qgen$(OBJ) rnd$(OBJ) varsub$(OBJ) \
	text$(OBJ) bcd2$(OBJ) permute$(OBJ) speed_seed$(OBJ) rng64$(OBJ)
OBJ3 = load_tbl$(OBJ) time$(OBJ) 
OBJS = $(OBJ1) $(OBJ2) $(OBJ3)
#
SETS = dists.dss 
DOC=README HISTORY PORTING.NOTES BUGS
DDL  = dss.ddl dss.ri
WINDOWS_IDE = tpch.dsw dbgen.dsp
OTHER=makefile.suite $(SETS) $(DDL) $(WINDOWS_IDE)
# case is *important* in TEST_RES
TEST_RES = O.res L.res c.res s.res P.res S.res n.res r.res
#
DBGENSRC=$(SRC1) $(HDR1) $(OTHER) $(DOC) $(SRC2) $(HDR2) $(SRC3)
FQD=queries/1.sql queries/2.sql queries/3.sql queries/4.sql queries/5.sql queries/6.sql queries/7.sql \
	queries/8.sql queries/9.sql queries/10.sql queries/11.sql queries/12.sql queries/13.sql \
	queries/14.sql queries/15.sql queries/16.sql queries/17.sql queries/18.sql queries/19.sql queries/20.sql \
	queries/21.sql queries/22.sql
VARIANTS= variants/8a.sql variants/12a.sql variants/13a.sql variants/14a.sql variants/15a.sql 
ANS   = answers/1.ans answers/2.ans answers/3.ans answers/4.ans answers/5.ans answers/6.ans answers/7.ans answers/8.ans \
	answers/9.ans answers/10.ans answers/11.ans answers/12.ans answers/13.ans answers/14.ans answers/15.ans \
	answers/16.ans answers/17.ans answers/18.ans answers/19.ans answers/20.ans answers/21.ans answers/22.ans
QSRC  = $(FQD) $(VARIANTS) $(ANS)
TREE_DOC=tree.readme tree.changes appendix.readme appendix.version answers.readme queries.readme variants.readme
REFERENCE=reference/*
ALLSRC=$(DBGENSRC) $(QSRC) $(REFERENCE) 
JUNK  = 
#
.PHONY: all
all: $(PROGS)
$(PROG1): $(OBJ1) $(SETS) 
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJ1) $(LIBS)
$(PROG2): permute.h $(OBJ2) 
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJ2) $(LIBS)
$(PROG3): $(OBJ3) $(SETS) 
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJ3) $(LIBS) $(SYSLIBS)
.PHONY: clean
clean:
	rm -f $(PROGS) $(OBJS) $(JUNK) time.c *.tbl* delete.* qset_* load.output
lint:
	lint $(CFLAGS) -u -x -wO -Ma -p $(SRC1)
	lint $(CFLAGS) -u -x -wO -Ma -p $(SRC2)

rnd$(OBJ): rnd.h
$(OBJ1): $(HDR1)
$(OBJ2): dss.h tpcd.h config.h rng64.h release.h

time.c: $(TOP)/binsrc/tests/time.c
	rm -f $@
	$(LN) $< $@
