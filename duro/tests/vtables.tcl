#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test create, insert with UNION, INTERSECT, JOIN, RENAME
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

# Create real tables
duro::table create T1 {
   {K INTEGER}
   {S1 STRING}
} {{K}} $tx

duro::table create T2 {
   {K INTEGER}
   {S1 STRING}
} {{K}} $tx

duro::table create T3 {
   {K INTEGER}
   {S2 STRING}
} {{K}} $tx

# Insert tuples
set r [duro::insert T1 {K 1 S1 Bla} $tx]
if {$r != 0} {
    puts "Insert returned $r, should be 0"
    exit 1
}

duro::insert T1 {K 2 S1 Blubb} $tx

duro::insert T2 {K 1 S1 Bla} $tx

duro::insert T2 {K 2 S1 Blipp} $tx

duro::insert T3 {K 1 S2 A} $tx

duro::insert T3 {K 2 S2 B} $tx

# Create virtual tables

duro::table expr -global TU {T1 UNION T2} $tx
duro::table expr -global TM {T1 MINUS T2} $tx
duro::table expr -global TI {T1 INTERSECT T2} $tx
duro::table expr -global TJ {TU JOIN T3} $tx
duro::table expr -global TR {T1 RENAME (K AS KN, S1 AS SN)} $tx
duro::table expr -global TX {EXTEND T1 ADD (K*10 AS K0)} $tx

duro::table expr -global TP {(T1 WHERE K = 1) {K}} $tx
duro::table expr -global TM2 {(T1 WHERE K = 1) MINUS (T2 WHERE K = 1)} $tx
duro::table expr -global TI2 {(T1 WHERE K = 1) INTERSECT (T2 WHERE K = 1)} $tx
duro::table expr -global TU2 {(T1 WHERE K = 1) UNION (T2 WHERE K = 1)} $tx
duro::table expr -global TJ2 {(T1 WHERE K = 2) JOIN (T3 WHERE K = 2)} $tx

if {[duro::expr {TUPLE FROM TP} $tx] != {K 1}} {
    puts "Tuple value shoud be {K 1}, but is not"
    exit 1
}

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set tpl {K 0 S1 Bold S2 Z}

duro::insert TJ $tpl $tx

if {![duro::table contains TJ $tpl $tx]} {
    puts "Insert into TJ was not successful."
    exit 1
}

set tpl {K 3 S1 Blu}

duro::insert TI $tpl $tx

if {![duro::table contains TI $tpl $tx]} {
    puts "Insert into TI was not successful."
    exit 1
}

set tpl {K 4 S1 Buchara}

duro::insert TU $tpl $tx

if {![duro::table contains TU $tpl $tx]} {
    puts "Insert into TU was not successful."
    exit 1
}

set tpl {KN 5 SN Ballermann}

duro::insert TR $tpl $tx

if {![duro::table contains TR $tpl $tx]} {
    puts "Insert into TR was not successful."
    exit 1
}

set l {}
set da [duro::array create TR {KN asc} $tx]
duro::array foreach i $da {
    array set a $i
    lappend l $a(KN)
} $tx
duro::array drop $da

if {$l != {0 1 2 3 4 5}} {
    puts "l is $l, should be {0 1 2 3 4 5}"
    exit 1
}

duro::delete TR {KN = 1} $tx

set l {}
set da [duro::array create TR {KN asc} $tx]
duro::array foreach i $da {
    array set a $i
    lappend l $a(KN)
} $tx
duro::array drop $da

if {$l != {0 2 3 4 5}} {
    puts "l is $l, should be {0 2 3 4 5}"
    exit 1
}

duro::delete TM {K = 5} $tx

set tpl [duro::expr {TUPLE FROM TM} $tx]
set stpl {K 2 S1 Blubb}
if {![tequal $tpl $stpl]} {
    puts "TUPLE FROM TM should be $stpl, but is $tpl"
    exit 1
}

duro::delete TX {K0 = 40} $tx

set tpl [duro::expr {TUPLE FROM (TX WHERE K0 >= 30)} $tx]
set stpl {K 3 S1 Blu K0 30}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

duro::table drop TU $tx

duro::table drop TX $tx

# Recreate TU

duro::table expr -global TU {T1 UNION T2} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv
