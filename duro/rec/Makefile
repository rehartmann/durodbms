# $Id$

include ../make.vars

CC = gcc
CFLAGS = -I$(BDB_INCLUDE) -I.. $(COPTS)

RM = rm -f

OBJS = env.o recmap.o cursor.o index.o

all: $(OBJS)

env.o : env.c env.h
	$(CC) $(CFLAGS) -c env.c

recmap.o : recmap.c recmap.h
	$(CC) $(CFLAGS) -c recmap.c

cursor.o : cursor.c cursor.h recmap.h
	$(CC) $(CFLAGS) -c cursor.c

index.o : index.c index.h recmap.h
	$(CC) $(CFLAGS) -c index.c

clean:
	$(RM) $(OBJS) core

all: $(OBJS)
