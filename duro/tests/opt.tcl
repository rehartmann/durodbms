#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test query optimizer
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create TEST $dbenv

#
# Perform test with table with integer key
#

# Create tables
set tx [duro::begin $dbenv TEST]

duro::table create T1 {
   {A INTEGER}
   {B INTEGER}
} {{A}} $tx

duro::insert T1 {A 1 B 1} $tx

duro::table create T2 {
   {A INTEGER}
   {B INTEGER}
   {C INTEGER}
} {{A B}} $tx

duro::table create T3 {
   {A INTEGER}
   {B INTEGER}
   {C INTEGER}
} {{A B}} $tx

duro::table create T4 {
   {A INTEGER}
   {B INTEGER}
   {C INTEGER}
} {{A B}} $tx

duro::insert T2 {A 1 B 2 C 3} $tx
duro::insert T2 {A 2 B 2 C 3} $tx
duro::insert T2 {A 1 B 3 C 3} $tx

duro::insert T3 {A 2 B 2 C 3} $tx

duro::insert T4 {A 1 B 3 C 3} $tx

duro::expr {TUPLE FROM (T1 WHERE A=1 AND B=1)} $tx

duro::table expr t {T1 WHERE A=1 AND B=0} $tx
set a [duro::array create t $tx]
set len [duro::array length $a]
if {$len != 0} {
    puts "$len tuples selected, should be 0"
    exit 1
}
duro::array drop $a
duro::table drop t $tx

duro::table expr t1 {T2 MINUS T3} $tx

duro::table expr t2 {t1 WHERE A=1} $tx

duro::table expr t3 {t2 WHERE B=2} $tx

set tpl [duro::expr {TUPLE FROM t3} $tx]
array set ta $tpl
if {$ta(A) != 1 || $ta(B) != 2 || $ta(C) != 3} {
    puts "Invalid value for TUPLE FROM t3: $tpl"
    exit 1
}

set tpl [duro::expr {TUPLE FROM ((((T2 WHERE A=1) INTERSECT T4) UNION T3)
        WHERE B=2)} $tx]
array set ta $tpl
if {$ta(A) != 2 || $ta(B) != 2 || $ta(C) != 3} {
    puts "Invalid tuple value: $tpl"
    exit 1
}

set tpl [duro::expr {TUPLE FROM (T2 RENAME (A AS AA)) WHERE B=2 AND AA=1} $tx]
array set ta $tpl
if {$ta(AA) != 1 || $ta(B) != 2 || $ta(C) != 3} {
    puts "Invalid tuple value: $tpl"
    exit 1
}

set tpl [duro::expr {TUPLE FROM (EXTEND T2 ADD (A AS AA)) WHERE B=2 AND AA=1} \
        $tx]
array set ta $tpl
if {$ta(A) != 1 || $ta(B) != 2 || $ta(C) != 3 || $ta(AA) != 1} {
    puts "Invalid tuple value: $tpl"
    exit 1
}

duro::table drop t3 $tx
duro::table drop t2 $tx
duro::table drop t1 $tx

duro::table drop T1 $tx
duro::table drop T2 $tx
duro::table drop T3 $tx
duro::table drop T4 $tx

duro::commit $tx
