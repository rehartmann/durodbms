# $Id$

include ../make.vars

CC = gcc
CFLAGS = -I$(BDB_INCLUDE) $(COPTS)

RM = rm -f


OBJS = hashmap.o errors.o strfns.o

all: $(OBJS)

clean:
	$(RM) $(OBJS) core

hashmap.o : hashmap.c hashmap.h
	$(CC) $(CFLAGS) -c hashmap.c

errors.o : errors.c errors.h
	$(CC) $(CFLAGS) -c errors.c

strfns.o : strfns.c strfns.h
	$(CC) $(CFLAGS) -c strfns.c
