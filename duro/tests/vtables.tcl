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
duro::insert T1 {K 1 S1 Bla} $tx

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
    error "Tuple value shoud be {K 1}, but is not"
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
    error "Insert into TJ was not successful."
}

set da [duro::array create TJ {K asc S1 asc} $tx]
checkarray $da {{K 0 S1 Bold S2 Z} {K 1 S1 Bla S2 A} {K 2 S1 Blipp S2 B}
        {K 2 S1 Blubb S2 B}} $tx
duro::array drop $da

set tpl {K 3 S1 Blu}

duro::insert TI $tpl $tx

if {![duro::table contains TI $tpl $tx]} {
    error "Insert into TI was not successful."
}

set tpl {K 4 S1 Buchara}

duro::insert TU $tpl $tx

if {![duro::table contains TU $tpl $tx]} {
    error "Insert into TU was not successful."
}

set tpl {KN 5 SN Ballermann}

duro::insert TR $tpl $tx

if {![duro::table contains TR $tpl $tx]} {
    error "Insert into TR was not successful."
}

set da [duro::array create TR {KN asc} $tx]
checkarray $da {{KN 0 SN Bold} {KN 1 SN Bla} {KN 2 SN Blubb} {KN 3 SN Blu}
        {KN 4 SN Buchara} {KN 5 SN Ballermann}} $tx
duro::array drop $da

duro::delete TR {KN = 1} $tx

set da [duro::array create TR {KN asc} $tx]
checkarray $da {{KN 0 SN Bold} {KN 2 SN Blubb} {KN 3 SN Blu}
        {KN 4 SN Buchara} {KN 5 SN Ballermann}} $tx
duro::array drop $da

duro::delete TR {KN >= 4} $tx

set da [duro::array create TR {KN asc} $tx]
checkarray $da {{KN 0 SN Bold} {KN 2 SN Blubb} {KN 3 SN Blu}} $tx
duro::array drop $da

duro::delete TR {SN = "Bold"} $tx

set da [duro::array create TR {KN asc} $tx]
checkarray $da {{KN 2 SN Blubb} {KN 3 SN Blu}} $tx
duro::array drop $da

duro::delete TM {K = 5} $tx

set tpl [duro::expr {TUPLE FROM TM} $tx]
set stpl {K 2 S1 Blubb}
if {![tequal $tpl $stpl]} {
    error "TUPLE FROM TM should be $stpl, but is $tpl"
}

duro::delete TX {K0 = 40} $tx

set tpl [duro::expr {TUPLE FROM (TX WHERE K0 >= 30)} $tx]
set stpl {K 3 S1 Blu K0 30}
if {![tequal $tpl $stpl]} {
    error "Tuple should be $stpl, but is $tpl"
}

duro::table drop TU $tx

duro::table drop TX $tx

# Try to drop TX a second time
if {![catch {duro::table drop TX $tx}]} {
    error "Dropping TX should fail, but succeeded"
}

# Recreate TU

duro::table expr -global TU {T1 UNION T2} $tx

#
# Check if persistent virtual tables can depend on transient tables
#

duro::table create -local LT {
    {A INTEGER}
} {{A}} $tx

if {![catch {duro::table expr -global TL {LT { A }} $tx}]} {
    error "Creation of TL should fail, but succeeded"
}

duro::table expr LTL {LT { A }} $tx

if {![catch {duro::table drop LT $tx}]} {
    error "Dropping TL should fail, but succeeded"
}

duro::table drop LTL $tx
duro::table drop LT $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv
