# $Id$

# Default directory where Berkeley DB is installed
BDB_HOME = /usr/local/BerkeleyDB.4.3

AM_YFLAGS = -d

if WITH_TCL
dist_bin_SCRIPTS = tcl/duroadmin.tcl tcl/durodump.tcl
endif

if WITH_TCL
TCLLIBRARIES = libdurotcl.la
else
TCLLIBRARIES =
endif

# Shared libs for testing are listed here too, because check_LTLIBRARIES does
# not work
lib_LTLIBRARIES = libduro.la $(TCLLIBRARIES) \
        tests/libpoint.la tests/libplus.la

htmldir = $(prefix)

if WITH_TCL
TCLHTML = docs/tcl/index.html \
docs/tcl/array.html docs/tcl/begin.html docs/tcl/call.html \
docs/tcl/commit.html docs/tcl/db.html docs/tcl/delete.html \
docs/tcl/env.html docs/tcl/expr.html docs/tcl/insert.html \
docs/tcl/operator.html docs/tcl/ptable.html docs/tcl/rollback.html \
docs/tcl/txdb.html docs/tcl/table.html docs/tcl/update.html \
docs/tcl/type.html docs/tcl/indexcmd.html docs/tcl/dberror.html
else
TCLHTML =
endif

nobase_dist_html_DATA = docs/api-spec.html docs/relnotes.html \
docs/expressions.html docs/utils.html $(TCLHTML)

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
tests/test_defop tests/test_callop tests/test_print_opview \
tests/test_dropop tests/maptest tests/tupletest util/lstables

tests_libpoint_la_SOURCES = tests/point.c tests/point.h

tests_libplus_la_SOURCES = tests/plus.c

TESTS = tests/maptest tests/ctests.sh tests/rtables.tcl tests/ltables.tcl \
        tests/project.tcl tests/tuple.tcl tests/sdivide.tcl tests/invtable.tcl \
        tests/operator.tcl tests/vtables.tcl tests/db2.tcl tests/rel.tcl \
        tests/subset.tcl tests/utype.tcl tests/deedum.tcl tests/opt.tcl \
        tests/index.tcl tests/oindex.tcl tests/convert.tcl tests/string.tcl \
        tests/rollback.tcl tests/constraints.tcl tests/qqueries.tcl \
        tests/parse.tcl tests/massign.tcl

TESTS_ENVIRONMENT = LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(BDB_HOME)/lib

LDADD = -L$(BDB_HOME)/lib libduro.la -ldb @LIBS@

tests_test_pointtable_LDADD = $(LDADD) -lm

libduro_la_SOURCES = gen/hashmap.c gen/hashmapit.c gen/strfns.c \
        gen/hashtable.c gen/hashtabit.c \
        rec/cursor.c rec/env.c rec/index.c rec/recmap.c \
        rel/array.c rel/internal.h rel/database.c rel/operator.c \
        rel/expression.c rel/qresult.c rel/serialize.c \
        rel/serialize.h rel/table.c rel/stable.c rel/vtable.c rel/update.c \
        rel/insert.c rel/contains.c rel/transaction.c rel/delete.c \
        rel/tuple.c rel/type.c rel/object.c rel/catalog.c rel/optimize.c \
        rel/transform.c rel/constraint.c rel/assign.c rel/excontext.c \
        rel/catalog.h rel/match.c \
        dli/exparse.y dli/exlex.l dli/parse.c dli/tabletostr.c \
        dli/tabletostr.h 

nobase_include_HEADERS = gen/hashmap.h gen/hashmapit.h gen/hashtable.h \
        gen/hashtabit.h gen/strfns.h gen/types.h \
        rec/cursor.h rec/env.h rec/index.h rec/recmap.h \
        rel/rdb.h rel/typeimpl.h dli/parse.h

if WITH_TCL
libdurotcl_la_SOURCES = tcl/duro.c tcl/duro.h tcl/envcmd.c tcl/txcmd.c \
        tcl/tablecmd.c tcl/dbcmd.c tcl/arraycmd.c tcl/opcmd.c tcl/exprcmd.c \
        tcl/typecmd.c tcl/indexcmd.c tcl/insertcmd.c tcl/updatecmd.c \
        tcl/constrcmd.c tcl/massigncmd.c
endif

if WITH_TCL
libdurotcl_la_LIBADD = -L$(BDB_HOME)/lib libduro.la -ldb
endif

pkglibdir = $(prefix)/lib

if WITH_TCL
dist_pkglib_DATA = tcl/util.tcl
endif

INCLUDES = -I$(top_srcdir) -I$(BDB_HOME)/include

dist_check_SCRIPTS = $(TESTS) tests/testutil.tcl

dist_check_DATA = tests/lstables_output tests/test_2db_output \
tests/test_aggregate_output tests/test_binary_output tests/test_callop_output \
tests/test_create_view_output tests/test_defop_output \
tests/test_defpointtype_output tests/test_deftype_output \
tests/test_delete_output tests/test_dropop_output tests/test_extend_output \
tests/test_insert_output tests/test_intersect_output tests/test_join_output \
tests/test_keys_output tests/test_minus_output tests/test_null_output \
tests/test_pointtable_output tests/test_print_opview_output \
tests/test_print_view_output tests/test_project_output tests/test_ra_output \
tests/test_regexp_output tests/test_rename_output tests/test_select_output \
tests/test_srupdate_output tests/test_summarize_output tests/test_union_output \
tests/test_update_output tests/test_utypetable_output tests/tupletest_output

# Delete test libs
install-exec-hook:
	rm $(DESTDIR)$(prefix)/lib/libpoint.*
	rm $(DESTDIR)$(prefix)/lib/libplus.*

# Install Tcl package
if WITH_TCL
install-data-hook:
	LD_LIBRARY_PATH=$(BDB_HOME)/lib;export LD_LIBRARY_PATH; \
                echo pkg_mkIndex $(DESTDIR)$(prefix)/lib \
                libdurotcl.so util.tcl | tclsh
endif
