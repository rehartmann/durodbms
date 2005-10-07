#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test multiple assignment
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create $dbenv TEST

set tx [duro::begin $dbenv TEST]

# Create table
duro::table create T1 {
   {A INTEGER}
   {B STRING}
} {{A}} $tx

# Create table
duro::table create T2 {
   {A INTEGER}
   {B STRING}
} {{A}} $tx

# Create table
duro::table create T3 {
   {A INTEGER}
   {B STRING}
} {{A}} $tx

# Create table
duro::table create T4 {
   {A INTEGER}
   {B STRING}
} {{A}} $tx

duro::insert T1 {A 1 B Bla} $tx
duro::insert T1 {A 2 B Ble} $tx
duro::insert T2 {A 1 B Blo} $tx

if {![catch {
    duro::massign \
            { copy T1 T2 } \
            { copy T2 T1 } $tx
}]} {
    error "multiple assignment should fail, but succeeded"
}
set errcode [lindex $errorCode 1]
if {$errcode != "RDB_NOT_SUPPORTED"} {
    error "wrong error: $errcode"
}

duro::massign \
        {insert T3 {A 1 B Blu}} \
        {delete T1 {A=1}} \
        {update T2 B {"Bli"}} \
        {copy T4 {RELATION {TUPLE {A 1, B "b"}}}} $tx

set tpl [duro::expr {TUPLE FROM T1} $tx]
if {![tequal $tpl {A 2 B Ble}]} {
    error "invalid value of T1"
}

set tpl [duro::expr {TUPLE FROM T2} $tx]
if {![tequal $tpl {A 1 B Bli}]} {
    error "invalid value of T2"
}

set tpl [duro::expr {TUPLE FROM T3} $tx]
if {![tequal $tpl {A 1 B Blu}]} {
    error "invalid value of T3"
}

set tpl [duro::expr {TUPLE FROM T4} $tx]
if {![tequal $tpl {A 1 B b}]} {
    error "invalid value of T4"
}

duro::commit $tx

duro::env close $dbenv