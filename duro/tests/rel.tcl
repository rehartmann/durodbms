#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test tables with relation-valued attribute, GROUP and UNGROUP.
#

load .libs/libdurotcl.so

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

duro::table create T2 {
    {SCATTR INTEGER}
    {RLATTR {relation
        {SCATTR INTEGER}
        {B STRING}
    }}
} {{SCATTR}} $tx

duro::table create T3 {
    {A STRING}
    {B INTEGER}
    {C STRING}
} {{A B}} $tx

duro::table expr -global U1 {T1 UNGROUP RLATTR} $tx

if {![catch {
    duro::table expr -global U2 {T2 UNGROUP RLATTR} $tx
}]} {
    puts "Creation of U2 should fail, but did not"
    exit 1
}

duro::table expr -global G1 {T3 GROUP {B, C} AS BC} $tx

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

set v [duro::expr {(TUPLE FROM (T1 WHERE RLATTR = RELATION {
        TUPLE {A 1, B "x"}, TUPLE {A 2, B "y"}})).SCATTR} $tx]

if {$v != 2} {
    puts "SCATTR value should be 2, but is $v"
    exit 1
}

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

duro::update T1 {SCATTR = 2} RLATTR {RELATION {TUPLE {A 1, B "y"},
        TUPLE {A 1, B "z"}}} $tx

set da [duro::array create U1 {B asc} $tx]

array set ta [duro::array index $da 0 $tx]

if {[array size ta] != 3 || $ta(A) != 1 || $ta(B) != "y"
        || $ta(SCATTR) != 2} {
    puts "Incorrect tuple value #0 from U1: $tpl"
    exit 1
}

array set ta [duro::array index $da 1 $tx]

if {[array size ta] != 3 || $ta(A) != 1 || $ta(B) != "z"
        || $ta(SCATTR) != 2} {
    puts "Incorrect tuple value #1 from U1: $tpl"
    exit 1
}

set alen [duro::array length $da]

if {$alen != 2} {
    puts "Incorrect length of array from U1: $alen
    exit 1
}

duro::array drop $da

set tpl {B y A 1 SCATTR 2}
if {![duro::table contains U1 $tpl $tx]} {
    puts "U1 should contain $tpl, but does not"
    exit 1
}

set tpl {B x A 1 SCATTR 2}
if {[duro::table contains U1 $tpl $tx]} {
    puts "U1 should not contain $tpl, but does"
    exit 1
}

duro::insert T3 {A a B 2 C c} $tx
duro::insert T3 {A a B 3 C c} $tx
duro::insert T3 {A b B 2 C d} $tx

set tpl {A a BC {{B 2 C c} {B 3 C c}}}

if {![duro::table contains G1 $tpl $tx]} {
    puts "G1 should contain $tpl, but does not"
    exit 1
}

set tpl {A a BC {{B 2 C c}}}

if {[duro::table contains G1 $tpl $tx]} {
    puts "G1 should not contain $tpl, but does"
    exit 1
}

set cnt [duro::expr {COUNT(G1)} $tx]
if {$cnt != 2} {
    puts "G1 should contain 2 tuples, but contains $cnt"
    exit 1
}

set tpl [duro::expr {TUPLE FROM ((TUPLE FROM (G1 WHERE A = "b")).BC)} $tx]
array unset ta
array set ta $tpl
if {$ta(B) != 2 || $ta(C) != "d"} {
    puts "Incorrect value of BC: $tpl"
    exit 1
}

set cnt [duro::expr {COUNT((TUPLE FROM ((G1 WHERE A = "a") {BC})).BC)} $tx]
if {$cnt != 2} {
puts "Incorrect cardinality of BC: $cnt"
    exit 1
}

duro::commit $tx
