#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT(duro, 0.9, rhartmann@users.sourceforge.net)
AC_CONFIG_SRCDIR([util/lstables.c])

AM_INIT_AUTOMAKE

AC_PREFIX_DEFAULT(/usr/local/duro.0.9)

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL
AC_PROG_YACC
AC_PROG_LEX

# Checks for libltdl.
AC_CHECK_LIB([ltdl], [lt_dlinit])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
