#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT(duro, 0.6, rhartmann@users.sourceforge.net)
AC_CONFIG_SRCDIR([util/dropdb.c])

AM_INIT_AUTOMAKE

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB
AC_PROG_LIBTOOL

# Checks for libraries.
AC_CHECK_LIB([db], [db_env_create])
AC_CHECK_LIB([ltdl], [lt_dlinit])
AC_CHECK_LIB([m], [atan])

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([float.h limits.h malloc.h stdlib.h string.h regex.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC
AC_CHECK_FUNCS([memmove memset regcomp sqrt strchr])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
