#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test non-unique (user) indexes
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

duro::table create T1 {
   {A INTEGER}
   {B STRING}
   {C STRING}
} {{A}} $tx

duro::index create IX1 T1 {B asc} $tx

duro::insert T1 {A 1 B Bli C X} $tx
duro::insert T1 {A 2 B Bla C Y} $tx
duro::insert T1 {A 3 B Bla C X} $tx
duro::insert T1 {A 4 B Blubb C Y} $tx

duro::table expr t {T1 WHERE B="Bla"} $tx

set ta [duro::array create t {A asc} $tx]

set len [duro::array length $ta]
if {$len != 2} {
    puts "Array length is $len, should be 2"
    exit 1
}

set tpl [duro::array index $ta 0 $tx]
set stpl {A 2 B Bla C Y}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

set tpl [duro::array index $ta 1 $tx]
set stpl {A 3 B Bla C X}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

duro::array drop $ta
duro::table drop t $tx

duro::commit $tx

duro::env close $dbenv

set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set tpl [duro::expr {TUPLE FROM (T1 WHERE B="Bli")} $tx]
set stpl {B Bli A 1 C X}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

set tpl [duro::expr {TUPLE FROM (T1 WHERE A=4 AND B="Blubb")} $tx]
set stpl {A 4 B Blubb C Y}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

if {![duro::expr {IS_EMPTY (T1 WHERE B="Blubb" AND C="X")} $tx]} {
    puts "Expression should be empty, but is not"
    exit 1
}

duro::commit $tx
