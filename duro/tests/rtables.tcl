#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test create, insert, update with several kinds of real tables
#

load .libs/libdurotcl.so

source tests/testutil.tcl

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
   {STRATTR STRING}
   {INTATTR INTEGER}
} {{INTATTR}} $tx

# Insert tuple
set r [duro::insert T1 {INTATTR 1 STRATTR Bla} $tx]
if {$r != 0} {
    puts "Insert returned $r, should be 0"
    exit 1
}

# Insert tuple a second time
set r [duro::insert T1 {INTATTR 1 STRATTR Bla} $tx]
if {$r != 1} {
    puts "Insert returned $r, should be 1"
    exit 1
}

# Update nonkey attribute
duro::update T1 STRATTR {STRATTR || "x"} $tx

# Update key attribute
duro::update T1 INTATTR {INTATTR + 1} $tx

set t [duro::expr {TUPLE FROM T1} $tx]
set s {INTATTR 2 STRATTR Blax}

if {![tequal $t $s] } {
    puts "Tuple value is $t, should be $s"
    exit 1
}

# Remove tuple
duro::delete T1 $tx

# Drop table
duro::table drop T1 $tx

#
# Perform test with table with two keys, one 'atomic' and one compound
#

# Create table
duro::table create T2 {
   {STRATTR1 STRING}
   {INTATTR INTEGER}
   {STRATTR2 STRING}
   {STRATTR3 STRING}
} {{STRATTR1} {INTATTR STRATTR2}} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

# Insert tuple
duro::insert T2 {INTATTR 1 STRATTR1 Bla STRATTR2 Bli STRATTR3 Blubb} $tx

# Update nonkey attribute
duro::update T2 STRATTR3 {STRATTR3 || "x"} $tx

# Update key attribute
duro::update T2 INTATTR {INTATTR * 2} $tx

set t [duro::expr {TUPLE FROM T2} $tx]
set s {INTATTR 2 STRATTR1 Bla STRATTR2 Bli STRATTR3 Blubbx}

if {![tequal $t $s] } {
    puts "Tuple value is $t, should be $s"
    exit 1
}

# Should have no effect
duro::update T2 {STRATTR1 = "Bla" AND INTATTR = 3} STRATTR3 {"Blubb"} $tx

set v [duro::expr {(TUPLE FROM T2).STRATTR3} $tx]
if {$v != "Blubbx"} {
    puts "Value should be Blubbx, but is $v"
    exit 1
}

# Should update tuple
duro::update T2 {STRATTR1 = "Bla" AND INTATTR = 2} STRATTR3 {"Blubby"} $tx

set v [duro::expr {(TUPLE FROM T2).STRATTR3} $tx]
if {$v != "Blubby"} {
    puts "Value should be Blubby, but is $v"
    exit 1
}

# Tuple must not be deleted
duro::delete T2 {INTATTR = 2 AND STRATTR2 = "Blo"} $tx

set cnt [duro::expr {COUNT (T2)} $tx]
if {$cnt != 1} {
    puts "T2 should contain 1 tuple, but contains $cnt"
    exit 1
}

# Tuple must not be deleted
duro::delete T2 {INTATTR = 2 AND STRATTR2 = "Bli" AND STRATTR3 = "B"} $tx

if {[duro::expr {IS_EMPTY (T2)} $tx]} {
    puts "T2 should not be empty, but is"
    exit 1
}

# Tuple must be deleted
duro::delete T2 {INTATTR = 2 AND STRATTR2 = "Bli" AND STRATTR3 = "Blubby"} $tx

if {![duro::expr {IS_EMPTY (T2)} $tx]} {
    puts "T2 should be empty, but is not"
    exit 1
}

# Drop table
duro::table drop T2 $tx

duro::commit $tx

set tx [duro::begin $dbenv TEST]

# Recreate table
duro::table create T2 {
   {STRATTR1 STRING}
   {INTATTR INTEGER}
   {STRATTR2 STRING}
   {STRATTR3 STRING}
} {{STRATTR1} {INTATTR STRATTR2}} $tx

duro::commit $tx
