#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test query optimizer
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

#
# Perform test with table with integer key
#

# Create table
set tx [duro::begin $dbenv TEST]
duro::table create T1 {
   {K INTEGER}
   {V INTEGER}
} {{K}} $tx

# Insert tuple
duro::insert T1 {K 1 V 1} $tx

duro::expr {TUPLE FROM (T1 WHERE K=1 AND V=1)} $tx

duro::table expr t {T1 WHERE K=1 AND V=0} $tx
set a [duro::array create t $tx]
set len [duro::array length $a]
if {$len != 0} {
    puts "$len tuples selected, should be 0"
    exit 1
}
duro::array drop $a
duro::table drop t $tx

# Drop table
duro::table drop T1 $tx

duro::commit $tx
