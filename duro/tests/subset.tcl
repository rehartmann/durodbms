#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test SUBSET operator
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

# Create tables
set tx [duro::begin $dbenv TEST]

duro::table create T1 {
   {INTATTR INTEGER}
   {STRATTR STRING}
} {{INTATTR}} $tx

duro::table create T2 {
   {INTATTR INTEGER}
   {STRATTR STRING}
} {{INTATTR}} $tx

duro::table create -local T3 {
   {INTATTR INTEGER}
   {STRATTR STRING}
} {{INTATTR}} $tx

#
# Insert tuples
#

duro::insert T1 {INTATTR 1 STRATTR A} $tx
duro::insert T1 {INTATTR 2 STRATTR B} $tx

duro::insert T2 {INTATTR 1 STRATTR A} $tx

duro::insert T3 {INTATTR 1 STRATTR B} $tx

#
# Check SUBSET operator
#

if {![duro::expr {T2 SUBSET T1} $tx]} {
    puts "T2 is not subset of T1, but should"
    exit 1
}

if {[duro::expr {T3 SUBSET T1} $tx]} {
    puts "T3 is subset of T1, but should not"
    exit 1
}

duro::commit $tx

# Close DB environment
duro::env close $dbenv
