# $Id$

include ../make.vars

CFLAGS = -I$(BDB_INCLUDE) -I.. $(COPTS)
LDFLAGS = -L$(BDB_LIB) -L../lib -g
LDLIBS = -ldb -lduro
RM = rm -f

OBJS = lstables.o
EXECUTABLES = lstables droptable dropdb

all : $(EXECUTABLES) $(OBJS)

clean :
	$(RM) $(EXECUTABLES) $(OBJS) core

lstables.o : lstables.c
	$(CC) $(CFLAGS) -c lstables.c

lstables : lstables.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o lstables lstables.o \
        $(LDFLAGS) $(LDLIBS)

droptable : droptable.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o droptable droptable.c \
        $(LDFLAGS) $(LDLIBS)

dropdb : dropdb.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o dropdb dropdb.c \
        $(LDFLAGS) $(LDLIBS)
