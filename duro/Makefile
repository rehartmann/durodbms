# Makefile for all Duro components
#
# $Id$

include make.vars

CFLAGS = $(COPTS)

RM = rm -f

AR = ar
ARFLAGS = rs

LIBS = lib/libduro.so lib/libduro.a
SOREV = 0.0

.PHONY : gen rec rel util tests

OBJS = rel/getargs.o rel/database.o rel/tuple.o rel/table.o rel/array.o \
       rel/qresult.o rel/transaction.o rel/type.o rel/expression.o \
       rel/value.o rel/serialize.o \
       rec/env.o rec/recmap.o rec/cursor.o rec/index.o \
       gen/hashmap.o gen/errors.o gen/strfns.o

all: util

clean:
	$(RM) $(LIBS) core
	cd gen; $(MAKE) clean
	cd rec; $(MAKE) clean
	cd rel; $(MAKE) clean
	cd util; $(MAKE) clean
	cd tests; $(MAKE) clean

check:
	cd tests; $(MAKE) test

util: $(LIBS)
	cd util; $(MAKE) all

tests: $(LIBS)
	cd tests; $(MAKE) all

lib/libduro.so : gen rec rel
	$(CC) $(CFLAGS) -shared -o lib/libduro.so $(OBJS)

lib/libduro.a : gen rec rel
	$(AR) $(ARFLAGS) lib/libduro.a $(OBJS)

gen:
	cd gen; $(MAKE) all

rec:
	cd rec; $(MAKE) all

rel:
	cd rel; $(MAKE) all
