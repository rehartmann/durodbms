#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test create, insert, UNWRAP on table with tuple attribute
#

load .libs/libdurotcl.so

source tcl/util.tcl

proc tequal {t1 t2} {
    return [string equal [lsort $t1] [lsort $t2]]
}

proc checkarray {a l} {
    set alen [duro::array length $a]
    if {$alen != [llength $l]} {
        puts "# of tuples is $alen, expected [llength $l]"
        exit 1
    }
    for {set i 0} {$i < $alen} {incr i} {
        set t [duro::array index $a $i]
        set xt [lindex $l $i]
        if {![tequal $t $xt]} {
            puts "Tuple value is $t, expected $xt"
            exit 1
        }
    }
}   

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

# Create table with tuple attribute
duro::table create T1 {
    {SCATTR STRING}
    {TPATTR {tuple
        {A INTEGER}
        {B STRING}
        {T {tuple
            {C STRING}
        }}
    }}
} {{SCATTR}} $tx

duro::insert T1 {SCATTR Bla TPATTR {A 1 B Blubb T {C Blip}}} $tx

# Create UNWRAP table
duro::table expr -global T2 {T1 UNWRAP (TPATTR)} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set a [duro::array create T1 $tx]
checkarray $a { {SCATTR Bla TPATTR {T {C Blip} A 1 B Blubb}} }
duro::array drop $a

set a [duro::array create T2 $tx]
checkarray $a { {T {C Blip} SCATTR Bla A 1 B Blubb} }
duro::array drop $a

set tpl {T {C Blip} SCATTR Bla A 1 B Blubb}

if {![duro::table contains T2 $tpl $tx]} {
    puts "T2 should contain $tpl, but does not"
    exit 1
}

set tpl {T {C Blop} SCATTR Bla A 1 B Blubb}

if {[duro::table contains T2 $tpl $tx]} {
    puts "T2 should not contain $tpl, but does"
    exit 1
}

duro::table expr T3 {T1 WHERE TPATTR.A = 1} $tx

set a [duro::array create T3 $tx]
checkarray $a { {SCATTR Bla TPATTR {T {C Blip} A 1 B Blubb}} }
duro::array drop $a

# Check tuple operations

set tpl [duro::expr {TUPLE {A 1, B "Bee"} {B}} $tx]
set stpl {B Bee}
if {![tequal $tpl $stpl]} {
    puts "Tuple is $tpl, should be $stpl"
    exit 1
}

set tpl [duro::expr {TUPLE {A 1, B "Bee"} {ALL BUT B}} $tx]
set stpl {A 1}
if {![tequal $tpl $stpl]} {
    puts "Tuple is $tpl, should be $stpl"
    exit 1
}

set tpl [duro::expr {TUPLE {A 1} RENAME (A AS B)} $tx]
set stpl {B 1}
if {![tequal $tpl $stpl]} {
    puts "Tuple is $tpl, should be $stpl"
    exit 1
}

set tpl [duro::expr {TUPLE {A 1} JOIN TUPLE {B "Bee"}} $tx]
set stpl {A 1 B Bee}
if {![tequal $tpl $stpl]} {
    puts "Tuple is $tpl, should be $stpl"
    exit 1
}

set tpl [duro::expr {EXTEND TUPLE {A 1} ADD (A*2 AS B)} $tx]
set stpl {A 1 B 2}
if {![tequal $tpl $stpl]} {
    puts "Tuple is $tpl, should be $stpl"
    exit 1
}

set tpl [duro::expr {TUPLE {A 1, B "Bee"} WRAP ((A, B) AS T)} $tx]
set stpl {T {A 1 B Bee}}
if {![tequal $tpl $stpl]} {
    puts "Tuple is $tpl, should be $stpl"
    exit 1
}

set tpl [duro::expr {TUPLE {T TUPLE {A 1, B "Bee"}} UNWRAP (T)} $tx]
set stpl {A 1 B Bee}
if {![tequal $tpl $stpl]} {
    puts "Tuple is $tpl, should be $stpl"
    exit 1
}

# Drop tables

duro::table drop T3 $tx
duro::table drop T2 $tx
duro::table drop T1 $tx

duro::commit $tx
