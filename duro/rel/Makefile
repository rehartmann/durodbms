# $Id$

include ../make.vars

CC = gcc
CFLAGS = -I$(BDB_INCLUDE) -I.. $(COPTS)

RM = rm -f

OBJS = database.o tuple.o table.o array.o qresult.o transaction.o \
       type.o expression.o value.o getargs.o serialize.o

all : $(OBJS)

clean :
	$(RM) $(OBJS) core

database.o : database.c rdb.h internal.h serialize.h
	$(CC) $(CFLAGS) -c database.c

transaction.o : transaction.c rdb.h
	$(CC) $(CFLAGS) -c transaction.c

table.o : table.c rdb.h typeimpl.h internal.h
	$(CC) $(CFLAGS) -c table.c

type.o : type.c rdb.h
	$(CC) $(CFLAGS) -c type.c

value.o : value.c rdb.h
	$(CC) $(CFLAGS) -c value.c

tuple.o : tuple.c rdb.h
	$(CC) $(CFLAGS) -c tuple.c

expression.o : expression.c rdb.h
	$(CC) $(CFLAGS) -c expression.c

qresult.o : qresult.c rdb.h typeimpl.h internal.h
	$(CC) $(CFLAGS) -c qresult.c

array.o : array.c rdb.h internal.h
	$(CC) $(CFLAGS) -c array.c

getargs.o : getargs.c rdb.h
	$(CC) $(CFLAGS) -c getargs.c

serialize.o : serialize.c serialize.h rdb.h typeimpl.h
	$(CC) $(CFLAGS) -c serialize.c
