#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test tables with relation-valued attribute
#

load .libs/libdurotcl.so

source tcl/util.tcl

proc tequal {t1 t2} {
    return [string equal [lsort $t1] [lsort $t2]]
}

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

# Create table with relation attribute
duro::table create T1 {
    {SCATTR INTEGER}
    {RLATTR {relation
        {A INTEGER}
        {B STRING}
    }}
} {{SCATTR}} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set tpl {SCATTR 2 RLATTR {{A 1 B x} {A 2 B y}}}
duro::insert T1 $tpl $tx

if {![duro::table contains T1 $tpl $tx]} {
    puts "Table should contain $tpl, but does not"
    exit 1
}

duro::insert T1 {SCATTR 3 RLATTR {}} $tx

duro::table expr X {EXTEND T1 ADD (COUNT(RLATTR) AS RCNT,
        IS_EMPTY(RLATTR) AS REMPTY)} $tx

# Get tuple with empty RLATTR

array set a [duro::expr {TUPLE FROM (X WHERE REMPTY)} $tx]

if {$a(SCATTR) != 3} {
    puts "SCATTR should be y, but is $a(SCATTR)"
    exit 1
}

if {$a(RLATTR) != {}} {
    puts "RLATTR should be empty, but is $a(SCATTR)"
    exit 1
}

# Get tuple with non-empty RLATTR

array set a [duro::expr {TUPLE FROM (X WHERE NOT REMPTY)} $tx]

if {$a(SCATTR) != 2} {
    puts "SCATTR should be x, but is $a(SCATTR)"
    exit 1
}

if {$a(RCNT) != 2} {
    puts "RCNT should be 2, but is $a(RCNT)"
    exit 1
}

# Test IN together with aggregate operators

set n [duro::expr {COUNT (T1 WHERE (TUPLE{A 1, B "y"} IN RLATTR))} $tx]
if {$n != 0} {
    puts "$n tuples found, should be 0"
    exit 1
}

set n [duro::expr {SUM (T1 WHERE (TUPLE{A 1, B "x"} IN RLATTR), SCATTR)} $tx]
if {$n != 2} {
    puts "SUM was $n, should be 2"
    exit 1
}

duro::update T1 {SCATTR = 2} RLATTR {RELATION {TUPLE {A 1, B "y"}}} $tx

set rel [lindex [duro::expr {TUPLE FROM ((T1 WHERE SCATTR = 2) {RLATTR})} $tx] 1]

if {![tequal [lindex $rel 0] {A 1 B y}]} {
    puts "Incorrect relation value: $rel"
    exit 1
}

duro::commit $tx
