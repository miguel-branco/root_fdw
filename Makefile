# contrib/root_fdw/Makefile

MODULE_big = root_fdw
OBJS = root_fdw.o
PG_CPPFLAGS = -I$(LIBROOTCURSOR)
SHLIB_LINK = -L$(LIBROOTCURSOR) -lrootcursor

EXTENSION = root_fdw
DATA = root_fdw--1.0.sql

REGRESS = root_fdw

EXTRA_CLEAN = sql/root_fdw.sql expected/root_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/root_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif