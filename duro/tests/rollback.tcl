#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test rollback
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create $dbenv TEST

set tx [duro::begin $dbenv TEST]

# Create table
duro::table create T {
   {A STRING}
   {B STRING}
} {{A}} $tx

duro::index create TIX T {B asc} $tx

duro::commit $tx

duro::env close $dbenv

set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

duro::expr {IS_EMPTY(T)} $tx

duro::rollback $tx

set tx [duro::begin $dbenv TEST]

duro::expr {IS_EMPTY(T)} $tx

duro::commit $tx

duro::env close $dbenv
