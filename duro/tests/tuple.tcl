#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test create, insert, update, UNWRAP on table with tuple attribute
# Test tuple project, EXTEND, JOIN, WRAP, and UNWRAP

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

#
# Test table UNWRAP
#

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

duro::insert T1 {SCATTR Bla TPATTR {A 1 B Blubb T {C Blap}}} $tx

if {[duro::expr {(TUPLE FROM (T1 {TPATTR})).TPATTR.T.C} $tx] != "Blap"} {
    puts "Attribute value should be \"Blap\""
    exit 1
}

# Update w/ WHERE
duro::update T1 {SCATTR="Bla"} TPATTR \
        {TUPLE {A 1, B "Blubb", T TUPLE {C "Blop"}}} $tx

if {[duro::expr {(TUPLE FROM (T1 {TPATTR})).TPATTR.T.C} $tx] != "Blop"} {
    puts "Attribute value should be \"Blop\""
    exit 1
}

# Update w/o WHERE
duro::update T1 TPATTR {TUPLE {A 1, B "Blubb", T TUPLE {C "Blip"}}} $tx

#
# Create UNWRAP table
#
duro::table expr -global UW {T1 UNWRAP (TPATTR)} $tx

#
# Test tuple comparison
#

duro::table expr -global WH \
    {(T1 WHERE TPATTR = TUPLE {T TUPLE {C "Blip"}, A 1, B "Blubb" }) \
    { SCATTR } } $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set tpl [duro::expr {TUPLE FROM T1} $tx]
set stpl {SCATTR Bla TPATTR {T {C Blip} A 1 B Blubb}}
if {![tequal $tpl $stpl]} {
    puts "Tuple value from T1 is $tpl, should be $stpl"
    exit 1
}

set tpl [duro::expr {TUPLE FROM UW} $tx]
set stpl {T {C Blip} SCATTR Bla A 1 B Blubb}
if {![tequal $tpl $stpl]} {
    puts "Tuple value from UW is $tpl, should be $stpl"
    exit 1
}

set tpl {T {C Blip} SCATTR Bla A 1 B Blubb}

if {![duro::table contains UW $tpl $tx]} {
    puts "UW should contain $tpl, but does not"
    exit 1
}

set tpl {T {C Blop} SCATTR Bla A 1 B Blubb}

if {[duro::table contains UW $tpl $tx]} {
    puts "UW should not contain $tpl, but does"
    exit 1
}

duro::table expr S {T1 WHERE TPATTR.A = 1} $tx

set tpl [duro::expr {TUPLE FROM S} $tx]
set stpl {SCATTR Bla TPATTR {T {C Blip} A 1 B Blubb}}
if {![tequal $tpl $stpl]} {
    puts "Tuple value from S is $tpl, should be $stpl"
    exit 1
}

# Test insert into UNWRAP table

set tpl {T {C Bleb} SCATTR Ble A 1 B Blubb}
duro::insert UW $tpl $tx
if {![duro::table contains UW $tpl $tx]} {
    puts "UW should contain $tpl, but does not"
    exit 1
}

#
# Test WRAP table
#

duro::table create T2 {
    {SATTR STRING}
    {IATTR INTEGER}
} {{SATTR}} $tx

duro::table expr -global WR {T2 WRAP ({SATTR, IATTR} AS A)} $tx

set tpl {A {SATTR a IATTR 1}}
duro::insert T2 {SATTR a IATTR 1} $tx
duro::insert WR $tpl $tx
if {![duro::table contains WR $tpl $tx]} {
    puts "W should contain $tpl, but does not"
    exit 1
}

#
# Test tuple operations
#

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

set tpl [duro::expr {TUPLE {A 1, B "Bee"} WRAP ({A, B} AS T)} $tx]
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


if {[duro::expr {TUPLE FROM WH} $tx] != "SCATTR Bla"} {
    puts "SCATTR should be \"Bla\", but is not"
    exit 1
}


# Drop tables
duro::table drop S $tx
duro::table drop UW $tx
duro::table drop WR $tx
duro::table drop WH $tx
duro::table drop T1 $tx
duro::table drop T2 $tx

duro::commit $tx
