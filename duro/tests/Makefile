# $Id$

include ../make.vars

CFLAGS = -I$(BDB_INCLUDE) -I.. $(COPTS)
LDFLAGS = -L$(BDB_LIB) -L../lib -g
LDLIBS = -lduro -ldb -lltdl -lm
RM = rm -f
LIBTOOL = libtool

OBJS =

EXECS = maptest tupletest prepare test_select test_minus test_union test_intersect \
       test_extend test_ra test_project test_join test_update test_delete \
       test_null test_keys test_aggregate test_create_view test_print_view \
       test_binary test_summarize test_rename test_regexp test_deftype test_utypetable \
       test_defpointtype test_pointtable

LIBS = libpoint.lo

all :	$(EXECS) $(LIBS)

clean :
	$(RM) *.o *.lo $(EXECS) $(LIBS) core
	$(RM) -r db

test : clean all
	LD_LIBRARY_PATH=$$LD_LIBRARY_PATH:$(BDB_LIB):../lib;\
        export LD_LIBRARY_PATH;\
        ./testall

prepare : prepare.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o prepare prepare.c $(LDLIBS)

test_select : test_select.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o test_select test_select.c $(LDLIBS)

test_minus : test_minus.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o test_minus test_minus.c $(LDLIBS)

test_union : test_union.c
	$(CC) -o test_union $(CFLAGS) $(LDFLAGS) test_union.c $(LDLIBS)

test_intersect : test_intersect.c
	$(CC) -o test_intersect $(CFLAGS) $(LDFLAGS) test_intersect.c $(LDLIBS)

test_extend : test_extend.c
	$(CC) -o test_extend $(CFLAGS) $(LDFLAGS) test_extend.c $(LDLIBS)

test_project : test_project.c
	$(CC) -o test_project $(CFLAGS) $(LDFLAGS) test_project.c $(LDLIBS)

test_ra : test_ra.c
	$(CC) -o test_ra $(CFLAGS) $(LDFLAGS) test_ra.c $(LDLIBS)

test_join : test_join.c
	$(CC) -o test_join $(CFLAGS) $(LDFLAGS) test_join.c $(LDLIBS)

test_update : test_update.c
	$(CC) -o test_update $(CFLAGS) $(LDFLAGS) test_update.c $(LDLIBS)

test_delete : test_delete.c
	$(CC) -o test_delete $(CFLAGS) $(LDFLAGS) test_delete.c $(LDLIBS)

test_null : test_null.c
	$(CC) -o test_null $(CFLAGS) $(LDFLAGS) test_null.c $(LDLIBS)

test_keys : test_keys.c
	$(CC) -o test_keys $(CFLAGS) $(LDFLAGS) test_keys.c $(LDLIBS)

test_aggregate : test_aggregate.c
	$(CC) -o test_aggregate $(CFLAGS) $(LDFLAGS) test_aggregate.c $(LDLIBS)

test_rename : test_rename.c
	$(CC) -o test_rename $(CFLAGS) $(LDFLAGS) test_rename.c $(LDLIBS)

test_summarize : test_summarize.c
	$(CC) -o test_summarize $(CFLAGS) $(LDFLAGS) test_summarize.c $(LDLIBS)

test_create_view : test_create_view.c
	$(CC) -o test_create_view $(CFLAGS) $(LDFLAGS) test_create_view.c $(LDLIBS)

test_print_view : test_print_view.c
	$(CC) -o test_print_view $(CFLAGS) $(LDFLAGS) test_print_view.c $(LDLIBS)

tupletest: tupletest.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o tupletest tupletest.c $(LDLIBS)

test_binary: test_binary.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o test_binary test_binary.c $(LDLIBS)

test_deftype: test_deftype.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o test_deftype test_deftype.c $(LDLIBS)

test_defpointtype: test_defpointtype.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o test_defpointtype test_defpointtype.c $(LDLIBS)

test_regexp : test_regexp.c
	$(CC) -o test_regexp $(CFLAGS) $(LDFLAGS) test_regexp.c $(LDLIBS)

test_utypetable: test_utypetable.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o test_utypetable test_utypetable.c $(LDLIBS)

test_pointtable: test_pointtable.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o test_pointtable test_pointtable.c $(LDLIBS)

point.lo : point.c
	$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c point.c

libpoint.lo : point.lo
	$(LIBTOOL) --mode=link $(CC) -g -module -o libpoint.la point.lo \
        -rpath $$PATH
