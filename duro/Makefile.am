# Default directory where Berkeley DB is installed
BDB_HOME = /usr/local/BerkeleyDB.4.1

bin_PROGRAMS = util/lstables util/droptable util/dropdb

# Shared libs for testing are listed here, because check_LTLIBRARIES does
# not work
lib_LTLIBRARIES = libduro.la \
        tests/libpoint.la tests/libplus.la

htmldir = $(prefix)/docs
html_DATA = docs/api-spec.html docs/relnotes.html
dist_html_DATA = docs/api-spec.html docs/relnotes.html

check_PROGRAMS = tests/prepare tests/test_aggregate tests/test_binary \
tests/test_create_view tests/test_defpointtype tests/test_deftype \
tests/test_delete tests/test_extend tests/test_intersect \
tests/test_join tests/test_keys tests/test_minus \
tests/test_null tests/test_pointtable tests/test_print_view \
tests/test_project tests/test_ra tests/test_regexp \
tests/test_rename tests/test_select tests/test_summarize \
tests/test_union tests/test_update tests/test_utypetable \
tests/test_defpointtype tests/test_pointtable \
tests/test_srupdate tests/test_insert tests/test_2db \
tests/test_defop tests/test_callop tests/test_print_opview tests/test_dropop

tests_libpoint_la_SOURCES = tests/point.c tests/point.h

tests_libplus_la_SOURCES = tests/plus.c

TESTS = tests/testall

TESTS_ENVIRONMENT = BDB_HOME=$(BDB_HOME)

LDADD = -L$(BDB_HOME)/lib libduro.la -ldb @LIBS@ -lmcheck

tests_test_pointtable_LDADD = $(LDADD) -lm

libduro_la_SOURCES = gen/errors.c gen/hashmap.c gen/strfns.c rec/cursor.c \
rec/env.c rec/index.c rec/recmap.c rel/array.c rel/internal.h rel/database.c \
rel/expression.c rel/getargs.c rel/qresult.c rel/serialize.c rel/serialize.h \
rel/table.c rel/transaction.c rel/tuple.c rel/type.c rel/object.c \
rel/catalog.c rel/catalog.h

nobase_include_HEADERS = gen/errors.h gen/hashmap.h gen/strfns.h gen/types.h \
rec/cursor.h rec/env.h rec/index.h rec/recmap.h \
rel/rdb.h rel/typeimpl.h

INCLUDES = -I$(top_srcdir) -I$(BDB_HOME)/include

# Delete test libs
install-exec-local:
	rm $(DESTDIR)$(prefix)/lib/libpoint.*
	rm $(DESTDIR)$(prefix)/lib/libplus.*
