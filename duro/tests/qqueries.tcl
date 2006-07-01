#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test quota queries
#

load .libs/libdurotcl.so

source tests/testutil.tcl

source tcl/util.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create $dbenv TEST

set tx [duro::begin $dbenv TEST]

# Create table
duro::table create T1 {
   {ID INTEGER}
   {WEIGHT INTEGER}
} {{ID}} $tx

# Create virtual table with WEIGHT_RANK attribute

duro::table expr V1 {EXTEND T1 ADD (
        COUNT(T1 RENAME (WEIGHT AS W) WHERE W < WEIGHT)
        AS WEIGHT_RANK)} $tx

duro::table expr V2 {V1 WHERE WEIGHT_RANK <= 1 { ID, WEIGHT } } $tx

# Insert tuples
duro::insert T1 {ID 1 WEIGHT 7} $tx
duro::insert T1 {ID 2 WEIGHT 7} $tx
duro::insert T1 {ID 3 WEIGHT 5} $tx
duro::insert T1 {ID 4 WEIGHT 20} $tx

set da [duro::array create V2 {WEIGHT asc ID asc} $tx]
checkarray $da {{ID 3 WEIGHT 5} {ID 1 WEIGHT 7} {ID 2 WEIGHT 7}} $tx
duro::array drop $da

duro::commit $tx

duro::env close $dbenv
