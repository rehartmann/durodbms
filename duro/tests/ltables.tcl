#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test create, insert, update, contains with local real table
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create TEST $dbenv

#
# Perform test with table with integer key
#

set tx [duro::begin $dbenv TEST]

# Create table
duro::table create -local L1 {
   {B STRING}
   {A INTEGER}
} {{A}} $tx

# Insert tuple
duro::insert L1 {A 1 B Bla} $tx

# Update nonkey attribute
duro::update L1 B {B || "x"} $tx

# Update key attribute
duro::update L1 A {A + 1} $tx

set stpl {A 2 B Blax}
if {![duro::table contains L1 $stpl $tx]} {
    error "L1 should contain $stpl, but does not"
}

set tpl [duro::expr {TUPLE FROM L1} $tx]
if {![tequal $tpl $stpl]} {
    error "TUPLE FROM L1 should be $stpl, but is $tpl"
}

duro::delete L1 $tx

if {![duro::expr {IS_EMPTY(L1)} $tx]} {
    error "L1 should be empty, but is not"
}

duro::table drop L1 $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv