#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test user-defined types
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create $dbenv TEST

set tx [duro::begin $dbenv TEST]

duro::table expr tdee TABLE_DEE $tx

if {[duro::expr {TUPLE FROM tdee} $tx] != ""} {
    error "Incorrect TABLE_DEE value"
}

duro::table expr tdum TABLE_DUM $tx

if {[duro::expr COUNT(tdum) $tx] != 0} {
    error "TABLE_DUM is not empty"
}

duro::commit $tx

# Close DB environment
duro::env close $dbenv
