#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test constraints
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

duro::table create T1 {
   {A INTEGER}
   {B INTEGER}
} {{A}} $tx

duro::table create T2 {
   {B INTEGER}
   {C INTEGER}
} {{B}} $tx

# Create 'tuple' constraint
duro::constraint create C1 {IS_EMPTY(T2 WHERE C>100)} $tx

# Create foreign key constraint
duro::constraint create C2 {IS_EMPTY((T1 {B}) MINUS (T2 {B}))} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

# Must succeed
duro::insert T2 {B 1 C 1} $tx

# Must fail
if {![catch {
    duro::insert T2 {B 2 C 200} $tx
}]} {
    error "insert should have failed, but succeded"
}

set code [lindex $errorCode 1]
if {$code != "RDB_PREDICATE_VIOLATION"} {
    error "wrong error code: $code"
}
set con [lindex $errorCode 3]
if {$con != "C1"} {
    error "wrong error constraint: $con"
}

# Must succeed
duro::insert T1 {A 1 B 1} $tx

# Must fail
if {![catch {
    duro::insert T1 {A 2 B 2} $tx
}]} {
    error "insert should have failed, but succeded"
}

set code [lindex $errorCode 1]
if {$code != "RDB_PREDICATE_VIOLATION"} {
    error "wrong error code: $code"
}
set con [lindex $errorCode 3]
if {$con != "C2"} {
    error "wrong error constraint: $con"
}

# Must fail
if {![catch {
    duro::update T2 {B = 1} C 200 $tx
}]} {
    error "update should have failed, but succeded"
}

set code [lindex $errorCode 1]
if {$code != "RDB_PREDICATE_VIOLATION"} {
    error "wrong error code: $code"
}
set con [lindex $errorCode 3]
if {$con != "C1"} {
    error "wrong error constraint: $con"
}

# Must fail
if {![catch {
    duro::delete T2 {B = 1} $tx
}]} {
    error "delete should have failed, but succeded"
}

set code [lindex $errorCode 1]
if {$code != "RDB_PREDICATE_VIOLATION"} {
    error "wrong error code: $code"
}
set con [lindex $errorCode 3]
if {$con != "C2"} {
    error "wrong error constraint: $con"
}

duro::table create T3 {
   {A INTEGER}
   {B INTEGER}
} {{A}} $tx

duro::table create T4 {
   {A INTEGER}
   {B INTEGER}
} {{A}} $tx

duro::table expr T5 {T3 INTERSECT T4} $tx

duro::insert T3 {A 1 B 1} $tx
duro::insert T3 {A 2 B 1} $tx

duro::constraint create C3 {SUM(T3, B) < 4} $tx

duro::update T3 {A = 1} B 2 $tx

# Must fail
if {![catch {
    duro::insert T5 {A 3 B 3} $tx
}]} {
    error "insert should have failed, but succeded"
}

set code [lindex $errorCode 1]
if {$code != "RDB_PREDICATE_VIOLATION"} {
    error "wrong error code: $code"
}
set con [lindex $errorCode 3]
if {$con != "C3"} {
    error "wrong error constraint: $con"
}

duro::commit $tx
