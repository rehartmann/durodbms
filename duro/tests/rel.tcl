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

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create $dbenv TEST

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
    error "Creation of U2 should fail, but did not"
}

duro::table expr -global G1 {T3 GROUP {B, C} AS BC} $tx

duro::table expr -global S1 {T1 WHERE RLATTR = RELATION {
        TUPLE {A 1, B "x"}, TUPLE {A 2, B "y"}}} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set tpl {SCATTR 2 RLATTR {{A 1 B x} {A 2 B y}}}
duro::insert T1 $tpl $tx

if {![duro::table contains T1 $tpl $tx]} {
    error "Table should contain $tpl, but does not"
}

# Try different order of tuples in attribute
set tpl {SCATTR 2 RLATTR {{A 2 B y} {A 1 B x}}}
if {![duro::table contains T1 $tpl $tx]} {
    error "Table should contain $tpl, but does not"
}

duro::insert T1 {SCATTR 3 RLATTR {}} $tx

set v [duro::expr {(TUPLE FROM S1).SCATTR} $tx]

if {$v != 2} {
    error "SCATTR value should be 2, but is $v"
}

duro::table expr X {EXTEND T1 ADD (COUNT(RLATTR) AS RCNT,
        IS_EMPTY(RLATTR) AS REMPTY)} $tx

# Get tuple with empty RLATTR

array set a [duro::expr {TUPLE FROM (X WHERE REMPTY)} $tx]

if {$a(SCATTR) != 3} {
    error "SCATTR should be y, but is $a(SCATTR)"
}

if {$a(RLATTR) != {}} {
    error "RLATTR should be empty, but is $a(SCATTR)"
}

# Get tuple with non-empty RLATTR

array set a [duro::expr {TUPLE FROM (X WHERE NOT REMPTY)} $tx]

if {$a(SCATTR) != 2} {
    error "SCATTR should be x, but is $a(SCATTR)"
}

if {$a(RCNT) != 2} {
    error "RCNT should be 2, but is $a(RCNT)"
}

# Test IN together with aggregate operators

set n [duro::expr {COUNT (T1 WHERE (TUPLE{A 1, B "y"} IN RLATTR))} $tx]
if {$n != 0} {
    error "$n tuples found, should be 0"
}

set n [duro::expr {SUM (T1 WHERE (TUPLE{A 1, B "x"} IN RLATTR), SCATTR)} $tx]
if {$n != 2} {
    error "SUM was $n, should be 2"
}

duro::update T1 {SCATTR = 2} RLATTR {RELATION {TUPLE {A 1, B "y"},
        TUPLE {A 1, B "z"}}} $tx

set da [duro::array create U1 {B asc} $tx]

array set ta [duro::array index $da 0 $tx]

if {[array size ta] != 3 || $ta(A) != 1 || $ta(B) != "y"
        || $ta(SCATTR) != 2} {
    error "Incorrect tuple value #0 from U1: $tpl"
}

array set ta [duro::array index $da 1 $tx]

if {[array size ta] != 3 || $ta(A) != 1 || $ta(B) != "z"
        || $ta(SCATTR) != 2} {
    error "Incorrect tuple value #1 from U1: $tpl"
}

set alen [duro::array length $da]

if {$alen != 2} {
    error "Incorrect length of array from U1: $alen"
}

duro::array drop $da

set tpl {B y A 1 SCATTR 2}
if {![duro::table contains U1 $tpl $tx]} {
    error "U1 should contain $tpl, but does not"
}

set tpl {B x A 1 SCATTR 2}
if {[duro::table contains U1 $tpl $tx]} {
    error "U1 should not contain $tpl, but does"
}

duro::insert T3 {A a B 2 C c} $tx
duro::insert T3 {A a B 3 C c} $tx
duro::insert T3 {A b B 2 C d} $tx

set tpl {A a BC {{B 2 C c} {B 3 C c}}}

if {![duro::table contains G1 $tpl $tx]} {
    error "G1 should contain $tpl, but does not"
}

set tpl {A a BC {{B 2 C c}}}

if {[duro::table contains G1 $tpl $tx]} {
    error "G1 should not contain $tpl, but does"
}

set cnt [duro::expr {COUNT(G1)} $tx]
if {$cnt != 2} {
    error "G1 should contain 2 tuples, but contains $cnt"
}

set tpl [duro::expr {TUPLE FROM ((TUPLE FROM (G1 WHERE A = "b")).BC)} $tx]
array unset ta
array set ta $tpl
if {$ta(B) != 2 || $ta(C) != "d"} {
    error "Incorrect value of BC: $tpl"
}

set cnt [duro::expr {COUNT((TUPLE FROM ((G1 WHERE A = "a") {BC})).BC)} $tx]
if {$cnt != 2} {
    error "Incorrect cardinality of BC: $cnt"
}

duro::commit $tx
